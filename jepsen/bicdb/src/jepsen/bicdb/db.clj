(ns jepsen.bicdb.db
  "Starts and stops BicDB consensus nodes as local processes.

   Jepsen normally reaches DB nodes over SSH. These tests run every node on
   one host as an ordinary process, so the test runs with :ssh {:dummy? true}
   and this namespace shells out directly. Node names are logical (\"n1\") and
   map to a consensus port and a pgwire port."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]))

(def binary
  "Path to the bicdb executable under test."
  (or (System/getenv "BICDB_BIN") "/root/bicdb/target/debug/bicdb"))

(def data-root "/tmp/jepsen-bicdb")

(def cluster-id "jepsen")

(defn node-index [node] (Integer/parseInt (subs (name node) 1)))
(defn consensus-port [node] (+ 5940 (node-index node)))
(defn pg-port       [node] (+ 5950 (node-index node)))
(defn data-dir      [node] (str data-root "/" (name node)))
(defn log-file      [node] (str data-root "/" (name node) ".log"))

(defn- sh! [& args]
  (let [r (apply shell/sh args)]
    (when-not (zero? (:exit r))
      (warn "command failed" (pr-str args) (:out r) (:err r)))
    r))

(defn pids
  "PIDs of running bicdb processes for this node, by data directory."
  [node]
  (->> (:out (shell/sh "bash" "-c" (str "pgrep -f 'consensus run " (data-dir node) " ' || true")))
       str/split-lines
       (remove str/blank?)
       (mapv #(Integer/parseInt (str/trim %)))))

(defn kill-node!
  "Sends signal to every process serving this node."
  [node signal]
  (doseq [pid (pids node)]
    (sh! "bash" "-c" (str "kill -" signal " " pid))))

(defn start-node!
  [node nodes]
  (let [peers (->> nodes
                   (map (fn [n] (str "--peer " (name n) "=127.0.0.1:" (consensus-port n))))
                   (str/join " "))
        cmd (str binary " consensus run " (data-dir node)
                 " --listen 127.0.0.1:" (consensus-port node)
                 " --node-id " (name node)
                 " --cluster-id " cluster-id
                 " --dev-localhost-plaintext"
                 " --pg-listen 127.0.0.1:" (pg-port node)
                 " " peers
                 " >> " (log-file node) " 2>&1 &")]
    (info "starting" (name node) "pg" (pg-port node))
    (sh! "bash" "-c" cmd)))

(defn wipe! [node]
  (kill-node! node "KILL")
  (Thread/sleep 300)
  (sh! "bash" "-c" (str "rm -rf " (data-dir node)))
  (sh! "bash" "-c" (str "mkdir -p " (data-dir node))))

(defn db
  "A BicDB consensus cluster of local processes."
  []
  (reify db/DB
    (setup! [_ test node]
      (wipe! node)
      (start-node! node (:nodes test))
      ; Give the node time to bind its ports and hold an election.
      (Thread/sleep 8000))

    (teardown! [_ test node]
      (kill-node! node "KILL")
      (Thread/sleep 200))

    db/LogFiles
    (log-files [_ _ node]
      {(log-file node) "bicdb.log"})))
