(ns jepsen.turbolay.causal
  "Session-consistency workload for the documented bookmark contract.

   The docs say `causal` reads use the node's local durable view and 'refresh
   only when a supplied bookmark requires a newer sequence'. That is a promise
   of two session guarantees, which this workload attacks directly by moving
   each session between nodes on every operation:

     read-your-writes  a read carrying the bookmark returned by an
                       acknowledged write must observe that write, even on a
                       different node;
     monotonic-reads   a session's reads must never lose an element it has
                       already observed.

   Ops carry a :session id so the checker can reconstruct per-session order
   even across client crashes."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.turbolay.http :as h]
            [jepsen.turbolay.routing :as routing]
            [jepsen.turbolay.edge-set :as es]))

(defrecord Client [node nodes session bookmark writer]
  client/Client
  (open! [this test node]
    (assoc this
           :node     node
           :nodes    (vec (:nodes test))
           :session  (str (java.util.UUID/randomUUID))
           :bookmark (atom nil)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      ;; Writes follow writer affinity, as a routing-aware driver would.
      :add
      (let [target (routing/write-node writer)
            op     (assoc op :session session :node target)
            res    (h/with-errors op
                     (let [r (h/query! target
                                       (str "CREATE (a {id: " es/root-id "})-[:" es/edge-type
                                            "]->(b {id: $v})")
                                       {:params   {:v (:value op)}
                                        :bookmark @bookmark
                                        :timeout  es/write-timeout})]
                       (when-let [b (:bookmark r)] (reset! bookmark b))
                       (assoc op :type :ok :bookmark (:bookmark r))))]
        (when (not= :ok (:type res)) (routing/advance! writer))
        res)

      ;; Reads deliberately land on a random node every time: a session that
      ;; never moves cannot detect a broken bookmark.
      :read
      (let [target (rand-nth nodes)
            op     (assoc op :session session :node target)]
        (h/with-errors op
          (let [r (h/query! target
                            (str "MATCH (a {id: " es/root-id "})-[:" es/edge-type "]->(b) "
                                 "RETURN b.id AS v")
                            {:consistency "causal"
                             :bookmark    @bookmark
                             :page-size   1000
                             :timeout     es/read-timeout})]
            (when-let [b (:bookmark r)] (reset! bookmark b))
            (assoc op
                   :type  :ok
                   :value (into (sorted-set) (map first (:rows r)))
                   :bookmark (:bookmark r)))))))

  (teardown! [this test])
  (close! [this test]))

(defn session-checker
  "Per-session read-your-writes + monotonic-reads over a grow-only set."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [;; :ok adds and all reads, in history order, grouped by session
            relevant (->> history
                          (filter #(#{:ok} (:type %)))
                          (filter :session))
            state    (reduce
                       (fn [st op]
                         (let [s   (:session op)
                               cur (get st s {:expect #{} :ryw [] :mono []})]
                           (case (:f op)
                             :add
                             (assoc st s (update cur :expect conj (:value op)))

                             :read
                             (let [seen    (set (:value op))
                                   missing (set/difference (:expect cur) seen)
                                   ;; elements this session wrote and had
                                   ;; acknowledged, but that vanished
                                   cur' (cond-> cur
                                          (seq missing)
                                          (update :ryw conj
                                                  {:session  s
                                                   :node     (:node op)
                                                   :index    (:index op)
                                                   :missing  (sort missing)
                                                   :bookmark (:bookmark op)}))
                                   ;; monotonic reads: never lose a previously
                                   ;; observed element
                                   lost (set/difference (:observed cur #{}) seen)
                                   cur' (cond-> cur'
                                          (seq lost)
                                          (update :mono conj
                                                  {:session s
                                                   :node    (:node op)
                                                   :index   (:index op)
                                                   :lost    (sort lost)}))]
                               (assoc st s (update cur' :observed
                                                   (fnil set/union #{}) seen)))
                             st)))
                       {}
                       relevant)
            ryw  (mapcat :ryw  (vals state))
            mono (mapcat :mono (vals state))]
        {:valid?                (and (empty? ryw) (empty? mono))
         :sessions              (count state)
         :read-your-writes-violations (take 20 ryw)
         :read-your-writes-count      (count ryw)
         :monotonic-read-violations   (take 20 mono)
         :monotonic-read-count        (count mono)}))))

(defn workload
  [opts]
  {:client  (map->Client {:writer (routing/writer-state (:nodes opts))})
   :checker (checker/compose
              {:session  (session-checker)
               :set-full (checker/set-full {:linearizable? false})})
   :generator (->> (gen/mix [(map (fn [v] {:f :add, :value v}) (range))
                             (repeat {:f :read})])
                   (gen/stagger 1/20))
   :final-generator (gen/each-thread {:f :read})})
