(ns jepsen.bicdb.append
  "Elle list-append workload over BicDB's SQL.

   Each Jepsen operation is one SQL transaction containing appends and reads.
   A key is a row in `append_log`; its list is a comma-separated text column,
   appended with `v = v || ',' || ?` and parsed back on read.

   BicDB documents READ COMMITTED and snapshot transaction behaviour, and
   explicitly does not claim serializability, so that is what is checked."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.bicdb.client :as c]
            [jepsen.client :as client]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def table "append_log")

(def opts {:builder-fn rs/as-unqualified-lower-maps})

(defn parse-list
  "',1,2' -> [1 2]"
  [s]
  (if (str/blank? s)
    []
    (->> (str/split s #",")
         (remove str/blank?)
         (mapv #(Long/parseLong (str/trim %))))))

(defn- read-key [tx k]
  (some-> (jdbc/execute-one! tx [(str "SELECT v FROM " table " WHERE id = " k)] opts)
          :v
          parse-list))

(defn- append-key!
  "Appends v to key k, creating the key if it does not exist.

   The update count is checked rather than assumed: an `UPDATE` against a row
   that does not exist affects zero rows and raises nothing, so without this a
   lost append would look like a successful one and the whole workload would
   quietly verify nothing."
  [tx k v]
  (jdbc/execute-one!
    tx [(str "INSERT INTO " table " (id, v) VALUES (" k ", '') "
             "ON CONFLICT (id) DO NOTHING")] opts)
  (let [r (jdbc/execute-one!
            tx [(str "UPDATE " table " SET v = v || ',' || '" v "' WHERE id = " k)] opts)
        n (:next.jdbc/update-count r)]
    (when-not (= 1 n)
      (throw (ex-info (str "append to key " k " affected " n " rows, expected 1")
                      {:key k :value v :update-count n})))))

(defn- apply-mop!
  [tx [f k v :as mop]]
  (case f
    :r      [:r k (or (read-key tx k) [])]
    :append (do (append-key! tx k v) mop)))

(defrecord AppendClient [ds node]
  client/Client
  (open! [this test node]
    (assoc this :ds (c/open node) :node node))

  (setup! [_ _] nil)

  (invoke! [_ _ op]
    (try
      (let [txn' (jdbc/with-transaction [tx ds]
                   (mapv (partial apply-mop! tx) (:value op)))]
        (assoc op :type :ok :value txn'))
      (catch Exception e
        (cond
          ; A follower refusing a write, or a rejected write-write conflict,
          ; both prove the transaction did not commit.
          (c/definite-failure? e)
          (assoc op :type :fail :error (c/failure-kind e))

          :else
          (assoc op :type :info :error (.getMessage e))))))

  (teardown! [_ _] nil)
  (close! [_ _] nil))

(defn provision!
  "Creates the table. Keys create themselves on first append."
  [node]
  (let [ds (c/open node)]
    (c/execute! ds [(str "CREATE TABLE " table " (id int PRIMARY KEY, v text)")])
    (info "provisioned" table)))

(defn client [] (map->AppendClient {}))
