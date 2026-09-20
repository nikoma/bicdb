(defproject jepsen.bicdb "0.1.0"
  :description "Jepsen tests for BicDB"
  :url "https://github.com/nikoma/bicdb"
  :license {:name "Apache-2.0" :url "https://github.com/nikoma/bicdb/blob/6aadb6da6e32c0c73cef77bad96889e9aca9baa6/LICENSE"}
  :main jepsen.bicdb.core
  :dependencies [[org.clojure/clojure "1.11.2"]
                 [jepsen "0.3.5"]
                 [org.postgresql/postgresql "42.7.3"]
                 [seancorfield/next.jdbc "1.2.659"]]
  :jvm-opts ["-Djava.awt.headless=true" "-Xmx4g"]
  :repl-options {:init-ns jepsen.bicdb.core})
