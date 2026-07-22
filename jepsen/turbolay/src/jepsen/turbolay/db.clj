(ns jepsen.turbolay.db
  "Installs, configures, starts and stops graph-node on each DB node.

   Every node points at the SAME object-store path, which is the whole point:
   SlateDB is the source of truth, one writer is fenced by epoch, and all five
   nodes serve reads. Faults are therefore expected to exercise writer
   handover rather than a consensus group."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [db :as db]
                    [control :as c]
                    [util :as util]]
            [jepsen.control.util :as cu]))

(def bin           "/usr/local/bin/graph-node")
(def logfile       "/var/log/graph-node.log")
(def pidfile       "/var/run/graph-node.pid")
(def token-file    "/etc/turbolay/auth-token")
(def cache-dir     "/var/cache/slatedb/data")

;; graph-node rejects tokens shorter than 32 characters, and rejects
;; "change-me" outright. Keep this at/above 32 chars.
(def auth-token    "jepsen-turbolay-bearer-token-0123456789")
(def graph-id      "default")
(def cell-id       "cell-0")
(def database      "default")

(def http-port  8443)
(def bolt-port  7687)
(def admin-port 9090)

(def minio-host "minio")
(def bucket     "jepsen")

(defn data-path
  "Object-store prefix for this test run.

   MUST be distinct per run. teardown! clears the local SlateDB cache but the
   object store is the source of truth, so a shared prefix carries the previous
   run's edges into the next one — which inflates result sets past the page
   size and makes checker/set-full compare against the wrong universe.
   :run-id is stamped into the test map by jepsen.turbolay/turbolay-test."
  [test]
  (str "jepsen/" (:run-id test "default") "/data"))

(defn bolt-node-addresses
  [test]
  (->> (:nodes test)
       (map (fn [n] (str n "=" n ":" bolt-port)))
       (str/join ",")))

(defn env
  "Environment for the graph-node process on `node`."
  [test node]
  {:CLOUD_PROVIDER                    "aws"
   :AWS_ACCESS_KEY_ID                 "jepsen"
   :AWS_SECRET_ACCESS_KEY             "jepsenjepsen"
   :AWS_REGION                        "us-east-1"
   :AWS_DEFAULT_REGION                "us-east-1"
   :AWS_ENDPOINT                      (str "http://" minio-host ":9000")
   :AWS_BUCKET                        bucket
   :AWS_ALLOW_HTTP                    "true"
   :AWS_VIRTUAL_HOSTED_STYLE_REQUEST  "false"

   :GRAPH_NODE_ID                     (name node)
   :GRAPH_NAMESPACE                   "default"
   :GRAPH_ID                          graph-id
   :GRAPH_CELL_ID                     cell-id
   :GRAPH_CELLS                       cell-id
   :GRAPH_DATABASE                    database
   :GRAPH_DATA_PATH                   (data-path test)
   :GRAPH_DATA_CACHE_DIR              cache-dir
   :GRAPH_DATA_CACHE_BYTES            "268435456"
   :GRAPH_ALLOW_PLAINTEXT             "true"
   :GRAPH_AUTH_TOKEN_FILE             token-file
   :GRAPH_BOLT_ADDR                   (str "0.0.0.0:" bolt-port)
   :GRAPH_HTTP_ADDR                   (str "0.0.0.0:" http-port)
   :GRAPH_ADMIN_ADDR                  (str "0.0.0.0:" admin-port)
   :GRAPH_ADVERTISED_BOLT_ADDR        (str (name node) ":" bolt-port)
   :GRAPH_BOLT_NODE_ADDRESSES         (bolt-node-addresses test)
   ;; Poll for freshly published CSC generations aggressively so index
   ;; publication races show up inside a short test run.
   :GRAPH_INDEX_DISCOVERY_INTERVAL_MS "1000"
   ;; Must be >= the largest timeout_ms any client requests: the service
   ;; rejects a requested runtime above this cap with 429 resource_exhausted
   ;; rather than clamping it (src/client/service.rs:774).
   :GRAPH_MAX_QUERY_RUNTIME_MS        "60000"
   :GRAPH_DEFAULT_PAGE_SIZE           "1000"
   :RUST_LOG                          "info"
   :RUST_BACKTRACE                    "1"})

(defn ready?
  "Is the node's admin server reporting ready?"
  []
  (try
    (= "200"
       (str/trim
         (c/exec :curl :-s :-o "/dev/null" :-w "%{http_code}"
                 (str "http://127.0.0.1:" admin-port "/readyz"))))
    (catch Exception _ false)))

(defn start!
  "Starts graph-node and waits (bounded) for it to serve /readyz.

   The wait matters: jepsen's db-nemesis fires :start and moves on immediately,
   so without it the post-fault reads race a process that has not bound its
   listener yet and come back :connection-refused — which looks like data loss
   to the checker rather than like a slow restart."
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   "/"
       :env     (env test node)}
      bin))
  (try
    (util/await-fn (fn [] (or (ready?) (throw (RuntimeException. "not ready"))))
                   {:log-message    (str "awaiting " node " /readyz")
                    :timeout        60000
                    :retry-interval 500})
    :started
    (catch Exception e
      (warn node "graph-node did not become ready within 60s")
      :start-timeout)))

(defn stop!
  [_test _node]
  (c/su
    (cu/stop-daemon! bin pidfile)))

(defn db
  "The Turbolay graph-node DB. The binary is baked into the node image, so
   `setup!` only writes secrets and launches the process."
  []
  (reify db/DB
    (setup! [this test node]
      (c/su
        (c/exec :mkdir :-p "/etc/turbolay" cache-dir)
        (c/exec :bash :-c (str "printf '%s' '" auth-token "' > " token-file))
        (c/exec :chmod "600" token-file))
      (start! test node)
      ;; Logged on every start so a path that silently changes mid-test — the
      ;; failure that once produced 426 phantom lost writes — is visible in
      ;; jepsen.log rather than only in the object store.
      (info node "graph-node data path" (data-path test))
      (info node "waiting for graph-node readiness")
      (util/await-fn (fn [] (or (ready?) (throw (RuntimeException. "not ready"))))
                     {:log-message (str "awaiting " node " /readyz")
                      :timeout     120000
                      :retry-interval 1000})
      (info node "graph-node ready"))

    (teardown! [this test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf cache-dir)
        (c/exec :mkdir :-p cache-dir)
        (c/exec :truncate :-s 0 logfile)))

    db/LogFiles
    (log-files [_ _ _] {logfile "graph-node.log"})

    db/Process
    (start! [_ test node] (start! test node))
    ;; stop-daemon! rather than grepkill!: grepkill! kills the process but
    ;; leaves /var/run/graph-node.pid behind, and start-stop-daemon then
    ;; reports :already-running against the stale pidfile and never restarts
    ;; the node. The run looks healed while the port stays closed.
    (kill!  [_ test node]
      (c/su (cu/stop-daemon! bin pidfile))
      :killed)

    db/Pause
    (pause!  [_ _ _] (c/su (cu/grepkill! :stop "graph-node")) :paused)
    (resume! [_ _ _] (c/su (cu/grepkill! :cont "graph-node")) :resumed)

    db/Primary
    ;; There is no elected primary: any node may lazily open the (fenced)
    ;; SlateDB writer. We report every node so nemeses that target "primaries"
    ;; degrade to targeting anyone.
    (primaries [_ test] (:nodes test))
    (setup-primary! [_ _ _])))
