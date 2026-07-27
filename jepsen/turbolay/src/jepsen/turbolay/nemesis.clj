(ns jepsen.turbolay.nemesis
  "Faults. Standard process/network/clock faults come from
   jepsen.nemesis.combined; this namespace adds the fault that is specific to
   an object-store-native database: cutting graph nodes off from S3 while
   leaving them reachable from clients and from each other.

   That fault is the interesting one. A node partitioned from the object store
   can still serve `causal` reads out of its local cache and can still accept
   Bolt/HTTP connections, so it is exactly the configuration in which a stale
   read or an unfenced write would go unnoticed in production."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nem]
                    [util :as util]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.turbolay.db :as tdb]))

(defn minio-ip
  "Resolves the object-store host from inside a DB node."
  []
  (-> (c/exec :getent :hosts tdb/minio-host)
      str/split-lines
      first
      (str/split #"\s+")
      first))

(defn drop-store!
  []
  (let [ip (minio-ip)]
    (c/su
      (c/exec :iptables :-A :INPUT  :-s ip :-j :DROP :-w)
      (c/exec :iptables :-A :OUTPUT :-d ip :-j :DROP :-w))
    ip))

(defn heal-store!
  []
  (c/su (c/exec :iptables :-F :-w) (c/exec :iptables :-X :-w))
  :healed)

(defn object-store-nemesis
  "Partitions a subset of nodes away from MinIO.

   ops: {:f :partition-store :value [node ...]} / {:f :heal-store}"
  []
  (reify nem/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        :partition-store
        (let [targets (:value op)]
          (assoc op :value
                 (c/on-nodes test targets (fn [_ _] (drop-store!)))))

        :heal-store
        (assoc op :value (c/on-nodes test (fn [_ _] (heal-store!))))))

    (teardown! [this test]
      (c/on-nodes test (fn [_ _] (heal-store!))))

    nem/Reflection
    (fs [this] #{:partition-store :heal-store})))

(defn store-generator
  "Alternating cut / heal cycle for the object-store fault.

   Each element is wrapped in gen/once: a bare function generator in Jepsen
   0.3 repeats forever, so an unwrapped cut would never let the heal run."
  [nodes interval]
  (->> (cycle
         [(gen/once
            (fn [_ _]
              {:type  :info
               :f     :partition-store
               ;; Cut a random minority-to-half of the nodes off from S3.
               :value (vec (take (max 1 (quot (count nodes) 2))
                                 (shuffle nodes)))}))
          (gen/once (fn [_ _] {:type :info, :f :heal-store}))])
       (gen/stagger interval)))

(defn package
  "Nemesis package for the requested fault set.

   Deliberately NOT nc/nemesis-package: that helper always constructs all five
   stock packages regardless of :faults, and the file-corruption nemesis
   downloads an x86_64-only `bitflip` release from GitHub during setup!. On
   arm64 nodes — or any host without egress — that aborts the run before a
   single operation is issued. Selecting packages by hand keeps setup to
   exactly the faults asked for."
  [{:keys [db nodes faults interval] :as opts}]
  (let [faults   (set faults)
        base     {:db        db
                  :nodes     nodes
                  :faults    faults
                  :partition {:targets [:one :majority :majorities-ring
                                        :primaries]}
                  :pause     {:targets [:one :primaries :all]}
                  :kill      {:targets [:one :primaries :all]}
                  :interval  interval}
        packages (cond-> []
                   (faults :partition)
                   (conj (nc/partition-package base))

                   (some faults [:kill :pause])
                   (conj (nc/db-package base))

                   (faults :clock)
                   (conj (nc/clock-package base))

                   (faults :object-store)
                   (conj {:nemesis         (object-store-nemesis)
                          :generator       (store-generator nodes interval)
                          :final-generator (gen/once {:type :info
                                                      :f    :heal-store})
                          :perf            #{{:name  "object-store"
                                              :start #{:partition-store}
                                              :stop  #{:heal-store}
                                              :color "#D1B2FF"}}}))]
    (if (empty? packages)
      nc/noop
      (nc/compose-packages packages))))
