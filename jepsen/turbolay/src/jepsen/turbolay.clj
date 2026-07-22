(ns jepsen.turbolay
  "Jepsen test entry point for Turbolay / slatedb-graph-kernel.

   Usage (from the control container):
     lein run test --workload edge-set --nemesis kill,partition,object-store \\
                   --time-limit 300 --concurrency 20"
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.os :as os]
            [jepsen.turbolay [db :as tdb]
                             [nemesis :as tnem]
                             [edge-set :as edge-set]
                             [register :as register]
                             [causal :as causal]]))

(def run-id
  "Object-store prefix discriminator, fixed once per JVM.

   This MUST NOT be computed inside turbolay-test. The test map is constructed
   more than once per `lein run` invocation, so a per-call timestamp produced
   several different values within a single test: setup! opened the database at
   one prefix, and a later nemesis-driven restart opened a *different, empty*
   prefix. The final reads then found an empty graph and checker/set-full
   reported 426 lost writes that were in fact sitting safely in the original
   prefix. A top-level def is evaluated once at namespace load, so every node
   and every restart in a run agrees on the path."
  (str (System/currentTimeMillis)))

(def workloads
  {:edge-set edge-set/workload
   :register register/workload
   :causal   causal/workload})

(def all-nemeses
  #{:partition :kill :pause :clock :object-store})

(defn parse-comma-kws
  [s]
  (if (or (nil? s) (= "none" s))
    #{}
    (->> (str/split s #",") (map keyword) set)))

(defn turbolay-test
  [opts]
  (let [wname    (:workload opts)
        wf       (get workloads wname)
        _        (assert wf (str "unknown workload " wname))
        workload (wf opts)
        db       (tdb/db)
        faults   (:nemesis opts)
        nemesis  (tnem/package {:db       db
                                :nodes    (:nodes opts)
                                :faults   faults
                                :interval (:nemesis-interval opts 15)})]
    (merge tests/noop-test
           opts
           {;; Isolates this run's object-store prefix from every other run.
            :run-id     run-id
            :name       (str "turbolay-" (name wname)
                             " " (:consistency opts "strong")
                             " " (if (seq faults)
                                   (str/join "," (map name (sort faults)))
                                   "no-faults"))
            ;; The node image already carries every dependency; letting
            ;; jepsen.os.debian re-provision would only add flakiness.
            :os         os/noop
            :db         db
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf       (checker/perf {:nemeses (:perf nemesis)})
                           :clock      (checker/clock-plot)
                           :stats      (checker/stats)
                           :exceptions (checker/unhandled-exceptions)
                           :workload   (:checker workload)})
            :generator
            (gen/phases
              (->> (:generator workload)
                   (gen/nemesis (:generator nemesis))
                   (gen/time-limit (:time-limit opts)))
              (gen/log "healing cluster")
              (gen/nemesis (:final-generator nemesis))
              (gen/log "waiting for recovery")
              (gen/sleep (:recovery-time opts 15))
              (when-let [f (:final-generator workload)]
                (gen/clients f)))})))

(def cli-opts
  [[nil "--workload NAME" "Workload: edge-set, register, causal"
    :default :edge-set
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--nemesis FAULTS" "Comma-separated faults, or none"
    :default #{:kill :partition :object-store}
    :parse-fn parse-comma-kws
    :validate [(fn [fs] (every? all-nemeses fs))
               (str "must be a subset of " (str/join "," (map name all-nemeses)))]]

   [nil "--nemesis-interval SECONDS" "Approx seconds between fault ops"
    :default 15
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--consistency MODE" "Read consistency: strong or causal"
    :default "strong"
    :validate [#{"strong" "causal"} "must be strong or causal"]]

   [nil "--recovery-time SECONDS" "Quiet period before the final read"
    :default 15
    :parse-fn read-string]

   [nil "--concurrency-per-key N" "Register workload: threads per key"
    :default 5
    :parse-fn read-string]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  turbolay-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
