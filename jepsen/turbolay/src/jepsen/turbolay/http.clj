(ns jepsen.turbolay.http
  "Thin client over the graph-node HTTPS query API:

     POST /v1/graphs/<graph_id>/query
     Authorization: Bearer <token>
     x-graph-namespace: <namespace>

   Response rows are typed cells, e.g. {\"type\":\"integer\",\"value\":7}."
  (:require [clojure.tools.logging :refer [info warn]]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen.turbolay.db :as tdb])
  (:import (java.net SocketTimeoutException ConnectException)
           (org.apache.http NoHttpResponseException)))

(defn url [node]
  (str "http://" (name node) ":" tdb/http-port
       "/v1/graphs/" tdb/graph-id "/query"))

(defn decode-cell
  "Typed JSON cell -> Clojure value."
  [cell]
  (when cell
    (let [v (get cell "value")]
      (case (get cell "type")
        "null"           nil
        "vertex_id"      v
        "integer"        v
        "signed_integer" v
        "float"          v
        "boolean"        v
        "string"         v
        "list"           (mapv decode-cell v)
        v))))

(defn- request!
  [node body timeout]
  (let [r (http/post (url node)
                     {:body               (json/generate-string body)
                      :content-type       :json
                      :accept             :json
                      :headers            {"Authorization"     (str "Bearer " tdb/auth-token)
                                           "x-graph-namespace" "default"}
                      :socket-timeout     timeout
                      :connection-timeout timeout
                      :throw-exceptions   true
                      :as                 :string})]
    (json/parse-string (:body r))))

(defn query!
  "Runs one Cypher statement, following server cursors until the result set is
   exhausted. Returns {:rows [[v ...] ...] :columns [...] :bookmark s
                       :read-epoch n}.

   A server cursor is bound to the query_id that created it, so every page of
   one logical query must carry the SAME query_id. Letting the server
   auto-generate one per request makes page 2 fail with
   `result cursor does not belong to this query request`, which silently
   truncates reads — and a truncated read looks exactly like catastrophic data
   loss to checker/set-full.

   opts: :params :consistency (\"causal\"|\"strong\") :bookmark :timeout
         :page-size :query-id"
  [node cypher {:keys [params consistency bookmark timeout page-size query-id]
                :or   {timeout 10000 page-size 1000}}]
  (let [query-id (or query-id (str "jepsen-" (java.util.UUID/randomUUID)))]
    (loop [cursor    nil
           acc       []
           columns   nil
           bookmark' bookmark
           epoch     nil]
      (let [body (cond-> {:cell_id    tdb/cell-id
                          :query      cypher
                          :query_id   query-id
                          :page_size  page-size
                          :timeout_ms timeout}
                   (seq params)   (assoc :parameters params)
                   consistency    (assoc :consistency consistency)
                   ;; A bookmark is only meaningful on the first page; later
                   ;; pages ride the server cursor's pinned snapshot.
                   (and bookmark (nil? cursor)) (assoc :bookmark bookmark)
                   cursor         (assoc :cursor cursor))
            resp (request! node body timeout)
            rows (->> (get resp "rows")
                      (mapv (fn [row] (mapv decode-cell row))))
            acc  (into acc rows)
            nxt  (get resp "next_cursor")]
        (if nxt
          (recur nxt acc (or columns (get resp "columns"))
                 (or (get resp "bookmark") bookmark')
                 (or epoch (get resp "read_epoch")))
          {:rows       acc
           :columns    (or columns (get resp "columns"))
           :bookmark   (or (get resp "bookmark") bookmark')
           :read-epoch (or (get resp "read_epoch") epoch)})))))

(defmacro with-errors
  "Maps transport/server failures onto Jepsen :fail / :info outcomes.

   Anything that could not have been applied is :fail; anything indeterminate
   (timeout, connection reset mid-flight, 5xx) must stay :info so the checker
   treats it as possibly-committed."
  [op & body]
  `(try ~@body
        (catch SocketTimeoutException e#
          (assoc ~op :type :info, :error :timeout))
        (catch NoHttpResponseException e#
          (assoc ~op :type :info, :error :no-http-response))
        (catch ConnectException e#
          (assoc ~op :type :fail, :error :connection-refused))
        (catch java.net.UnknownHostException e#
          (assoc ~op :type :fail, :error :unknown-host))
        (catch java.io.IOException e#
          (assoc ~op :type :info, :error [:io (.getMessage e#)]))
        (catch clojure.lang.ExceptionInfo e#
          (let [status# (:status (ex-data e#))
                body#   (:body (ex-data e#))
                parsed# (try (json/parse-string body#) (catch Exception _# nil))
                code#   (get-in parsed# ["error" "code"])
                ;; Keep the server's message: without it a 429 or 500 in the
                ;; history is undiagnosable without cross-referencing logs.
                msg#    (some-> (get-in parsed# ["error" "message"])
                                (subs 0 (min 160 (count (get-in parsed# ["error" "message"])))))
                code#   (if msg# [code# msg#] code#)]
            (cond
              ;; Client errors definitively did not mutate anything.
              (and status# (<= 400 status# 499))
              (assoc ~op :type :fail, :error [status# code#])

              ;; 503 from an overloaded/unready node: request rejected.
              (= 503 status#)
              (assoc ~op :type :fail, :error [:unavailable code#])

              ;; Everything else is indeterminate.
              :else
              (assoc ~op :type :info, :error [(or status# :unknown) code#]))))))
