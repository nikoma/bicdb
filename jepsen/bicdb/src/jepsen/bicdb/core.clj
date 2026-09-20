(ns jepsen.bicdb.core
  "Jepsen test entry point for BicDB."
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.bicdb.append :as append]
            [jepsen.bicdb.db :as bdb]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.bicdb.nemesis :as bnem]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests.cycle.append :as app]
            [jepsen.tests :as tests]))

(def key-count 5)

(defn bicdb-test
  [opts]
  (let [wl (app/test {:key-count         key-count
                      :min-txn-length    1
                      :max-txn-length    4
                      :consistency-models [(:consistency-model opts :read-committed)]})]
    (merge tests/noop-test
           opts
           {:name      "bicdb-append"
            ; Every node is a local process on this host, so jepsen.control
            ; has nothing to SSH into.
            :ssh       {:dummy? true}
            :os        jepsen.os/noop
            :db        (bdb/db)
            :client    (append/client)
            :nemesis   (if (:crash opts) (bnem/killer) nemesis/noop)
            :checker   (checker/compose
                         {:perf     (checker/perf)
                          :workload (:checker wl)})
            :generator (gen/phases
                         ; The cluster needs a settled leader, and the table
                         ; has to exist, before any history is recorded.
                         (gen/once
                           (fn [_ _]
                             (Thread/sleep 3000)
                             (append/provision! (first (:nodes opts)))
                             nil))
                         ; gen/clients matters: without it the nemesis process
                         ; is handed workload transactions it cannot run, and
                         ; every one of them lands in the history as an
                         ; indeterminate :info that Elle can conclude nothing
                         ; from. That alone was 590 of 1325 operations.
                         (->> (:generator wl)
                              gen/clients
                              (gen/stagger 1/50)
                              (gen/nemesis
                                (when (:crash opts)
                                  (cycle [(gen/sleep 8)
                                          {:type :info :f :kill}
                                          (gen/sleep 2)
                                          {:type :info :f :start}])))
                              (gen/time-limit (:time-limit opts 60))))})))

(def cli-opts
  [[nil "--crash" "Kill and restart nodes with SIGKILL during the run."]])

(defn -main [& args]
  (cli/run! (cli/single-test-cmd {:test-fn bicdb-test :opt-spec cli-opts})
            args))
