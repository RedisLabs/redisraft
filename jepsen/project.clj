(defproject jepsen.redisraft "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Redis Raft"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.redisraft
;; Uncomment this for more recent JVMs
;; :jvm-opts ["--add-modules" "java.xml.bind"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.11"]
                 [com.taoensso/carmine "2.19.1"]])
