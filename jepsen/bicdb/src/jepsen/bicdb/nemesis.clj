(ns jepsen.bicdb.nemesis
  "Faults for BicDB: kill a node with SIGKILL and bring it back.

   SIGKILL is the interesting one here. It is the fault that a write-ahead log
   exists to survive, and every acknowledged append must still be present when
   the node comes back."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.bicdb.db :as bdb]
            [jepsen.nemesis :as nemesis]))

(defn killer
  "Nemesis supporting :kill and :start of a node."
  []
  (reify nemesis/Nemesis
    (setup! [this _] this)

    (invoke! [_ test op]
      (let [node (rand-nth (vec (:nodes test)))]
        (case (:f op)
          :kill  (do (bdb/kill-node! node "KILL")
                     (assoc op :value (str "killed " (name node))))
          :start (do (bdb/start-node! node (:nodes test))
                     (Thread/sleep 6000)
                     (assoc op :value (str "started " (name node)))))))

    (teardown! [_ _] nil)))
