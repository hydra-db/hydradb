(defproject jepsen.turbolay "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the Turbolay / slatedb-graph-kernel graph engine"
  :url "https://github.com/usecortex/slatedb-graph-kernel"
  :license {:name "Proprietary"}
  :main jepsen.turbolay
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]]
  :jvm-opts ["-Xmx4g"
             "-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.turbolay})
