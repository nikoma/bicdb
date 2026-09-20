(ns jepsen.bicdb.client
  "A JDBC client for BicDB's PostgreSQL wire protocol.

   BicDB does not claim serializability, so these tests check the isolation it
   does claim. Writes are only accepted by the consensus leader; a follower
   refuses them, which is a legitimate outcome and is recorded as :fail (the
   write definitely did not happen) rather than :info."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.bicdb.db :as bdb]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(defn spec [node]
  {:dbtype "postgresql"
   :host "127.0.0.1"
   :port (bdb/pg-port node)
   :dbname "bicdb"
   :user "bicdb"
   :password "jepsen"
   :socketTimeout 10
   :connectTimeout 5})

(defn open [node]
  (jdbc/get-datasource (spec node)))

(defn not-leader?
  "True when an exception is BicDB refusing a write on a non-leader."
  [^Throwable e]
  (boolean (some-> e .getMessage (.contains "not the consensus leader"))))

(defn conflict?
  "True when BicDB rejected the transaction for a write-write conflict."
  [^Throwable e]
  (boolean (some-> e .getMessage (.contains "transaction conflict"))))

(defn unreachable?
  "True when the client never reached the server at all.

   A refused connection proves the transaction did not happen, so it belongs in
   the history as :fail. Recording it as :info instead would leave the checker
   unable to conclude anything across every restart window."
  [^Throwable e]
  (let [m (str (.getMessage e))]
    (or (.contains m "Connection refused")
        (.contains m "Connection to ")
        (.contains m "connection attempt failed"))))

(defn definite-failure?
  "True when the exception proves the operation did not take effect.

   These are refusals BicDB makes *before* committing anything, so the
   transaction definitely did not apply. Recording them as :fail rather than
   :info is what lets Elle reason about them instead of treating them as
   indeterminate."
  [^Throwable e]
  (or (not-leader? e)
      (conflict? e)
      (unreachable? e)
      (boolean (some-> e .getMessage (.contains "permission denied")))))

(defn failure-kind [^Throwable e]
  (cond (not-leader? e)  :not-leader
        (conflict? e)    :conflict
        (unreachable? e) :unreachable
        :else            :rejected))

(defn execute!
  [ds sql]
  (jdbc/execute! ds sql {:builder-fn rs/as-unqualified-lower-maps}))
