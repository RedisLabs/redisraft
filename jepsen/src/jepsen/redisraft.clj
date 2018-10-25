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
                    [tests :as tests]]
            [jepsen.control [util :as cu]
                            [net :as net]]
            [jepsen.os.debian :as debian]
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
   :spec {:uri (str "redis://:@" node ":" port)}})

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
        (str "raftlog=" raftlog))))

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

(defn redis-cas
  [key old new]
  (car/lua "local c = redis.call('get', _:key);
            if (c == _:old-val) then
                redis.call('set', _:key, _:new-val);
                return 1
            else
                return 0
            end"
           {:key key}
           {:old-val old
            :new-val new}))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (redis-conn node)))

  (setup! [this test])

  (invoke! [_ test op]
    (case (:f op)
      :read (assoc op :type :ok, :value (car/wcar conn (car/get "foo")))
      :write (do (car/wcar conn (car/set "foo" (:value op)))
                 (assoc op :type, :ok))
      :cas (let [[old new] (:value op)]
             (assoc op :type (if (= 1 (car/wcar conn (redis-cas "foo" old new)))
                               :ok
                               :fail)))))

  (teardown! [this test])

  (close! [_ test]))

(defn redisraft-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "redisraft"
          :os   debian/os
          :db   (db "1.0")
          :client (Client. nil)
          :checker (checker/linearizable)
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis nil)
                          (gen/time-limit 15))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn redisraft-test})
                   (cli/serve-cmd))
            args))

