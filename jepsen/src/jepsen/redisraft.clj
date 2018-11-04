(ns jepsen.redisraft
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [core :as jepsen]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.control [util :as cu]
                            [net :as net]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [taoensso.carmine :as car]))

(def dir "/opt/redisraft")
(def port 6379)
(def binary "redis-server")
(def control (str dir "/redis-cli"))
(def logfile (str dir "/redis.log"))
(def pidfile (str dir "/redis.pid"))
(def raftlog (str dir "/raftlog.db"))
(def modulename (str dir "/redisraft.so"))

(defn redis-conn
  "Returns a redis connection spec for node"
  [node]
  {:pool {}
   :spec {:host node
          :port port
          :timeout-ms 1000}})

(defn redisraft-node-ids
  "Returns a map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node (+ i 1)]))
       (into {})))

(defn redisraft-node-id
  "Given a test and a node name from that test, returns the ID for that node."
  [test node]
  ((redisraft-node-ids test) node))

(defn install!
  "Installs DB for the given node."
  [node version]
  (info node "installing redisraft" version)
  (c/su
    (let [url (str "file:///dist/redisraft-" version "-linux-amd64.tar.gz")]
      (cu/install-archive! url dir))))

(defn start!
  "Starts DB."
  [node test]
  (info node "starting redisraft")
  (c/su
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   dir}
      binary
      :--logfile logfile
      :--port    port
      :--bind    "0.0.0.0"
      :--loadmodule modulename
        (str "id=" (redisraft-node-id test node))
        (when-not (:disable-proxy test) "follower-proxy=yes")
        (when (:persistence test) (str "raftlog=" raftlog)))))

(defn create!
  "Create the redisraft cluster."
  [node test]
  (let [p (jepsen/primary test)]
    (when (= node p)
      (info node "creating redisraft cluster.")
      (let [res (c/exec control "-p" port :raft.cluster :init)]
       (assert (re-find #"^[0-9a-f]" res))))))

(defn join!
  "Joins DB nodes."
  [node test]
  (jepsen/synchronize test)
  (let [p (jepsen/primary test)]
    (when-not (= node p)
      (info node "joining redisraft cluster")
      (let [res (c/exec control "-p" port
                        :raft.cluster :join (str (net/ip (name p)) ":" port))]
        (assert (re-find #"^OK$" res))))))

(defn stop!
  "Stops DB."
  [node test]
  (info node "stopping redisraft")
  (cu/stop-daemon! binary pidfile)
  (c/su (c/exec :rm :-rf dir)))

(defn wipe!
  "Wipe data files."
  [node test]
  (info node "wiping data files")
  (c/su (c/exec :rm :-rf dir)))

(defn db
  "RedisRaft DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (start! test)
        (create! test)
        (join! test)))

    (teardown! [_ test node]
      (doto node
        (stop! test)
        (wipe! test)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn parse-long
  [s]
  (when s (Long/parseLong s)))

(defn redis-cas
  [key old new]
  (car/redis-call [:raft :eval "
            local c = redis.call('get', KEYS[1]);
            if (c == ARGV[1]) then
                redis.call('SET', KEYS[1], ARGV[2]);
                return 1
            else
                return 0
            end" 1 key old new]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (redis-conn node)))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k, v] (:value op)]
      (try
        (case (:f op)
          :read (let [value (-> (car/wcar conn (car/redis-call [:raft :get k]))
                                parse-long)]
                  (assoc op :type :ok, :value (independent/tuple k value)))
          :write (do (car/wcar conn (car/redis-call [:raft :set k v]))
                     (assoc op :type, :ok))
          :cas (let [[old new] v]
                 (assoc op :type (if (= 1 (car/wcar conn (redis-cas k old new)))
                                   :ok
                                   :fail))))
        (catch Exception e
          (let [error (.getMessage e)]
            (cond
              (re-find #"^NOLEADER" error) (assoc op :type :fail, :error :no-leader-elected)
              (re-find #"^MOVED" error) (assoc op :type :fail, :error :not-a-leader)
              (re-find #"^Read timed out" error) (assoc op :type :info, :error :timeout)
              :else (throw e)))))))

  (teardown! [this test])

  (close! [_ test]))

(defn redisraft-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name         (str "redisraft"
                             (when (:persistence opts) "+persistence")
                             (when (:disable-proxy opts) "+disable-proxy"))
          :os           debian/os
          :db           (db "1.0")
          :client       (Client. nil)
          :nemesis      (nemesis/partition-random-halves)
          :model        (model/cas-register)
          :checker (checker/compose
                     {:perf (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w cas])
                                   (gen/stagger 1/10)
                                   (gen/limit 100))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}))

(def cli-opts
  "Additional command line options."
  [["-p" "--persistence" "Use a persistent disk-based Raft log."
    :default false]
   [nil "--disable-proxy" "Do not Use request proxying from follower to leader."
    :default false]
   ])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn redisraft-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))

