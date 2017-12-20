(ns ^{:doc "Sparkpost.com Email API driver"}
  arctype.service.io.sparkpost
  (:require
    [clojure.core.async :as async]
    [cheshire.core :as json]
    [clojure.tools.logging :as log]
    [schema.core :as S]
    [schema.coerce :refer [coercer json-coercion-matcher]]
    [schema.utils :refer [error-val]]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.util :refer [error? map-vals xform-validator]]
    [arctype.service.io.http.client :as http-client]))

(def Config
  {:api-key S/Str
   (S/optional-key :sandbox?) S/Bool
   (S/optional-key :endpoint) S/Str
   (S/optional-key :http) http-client/Config})

(def default-config
  {:endpoint "https://api.sparkpost.com/api"
   :sandbox? false
   :http {:throttle
          {:rate 10
           :period :minute
           :burst 2}}})

(def From
  {:name S/Str
   :email S/Str})

(def InlineRecipient
  {:address {:email S/Str 
             (S/optional-key :name) S/Str}
   (S/optional-key :tags) [S/Str]
   (S/optional-key :metadata) {S/Keyword S/Any}
   (S/optional-key :substitution_data) {S/Keyword S/Any}})

(def Recipient
  (S/conditional
    #(some? (:address %)) InlineRecipient
    :else S/Str ; list_id
    ))

(def InlineContent
  {(S/optional-key :html) S/Str
   (S/optional-key :text) S/Str
   :subject S/Str
   :from From
   (S/optional-key :reply_to) S/Str
   (S/optional-key :headers) {S/Keyword S/Any}
   ; (S/optional-key :attachments) ...
   ; (S/optional-key :inline_images) ...
   })

(def Content
  (S/conditional
    #(some? (:subject %)) InlineContent
    :else {:template_id S/Str
           (S/optional-key :use_draft_template) S/Bool}))

(def Options
  {(S/optional-key :start_time) S/Str ; Delay generation of messages until this datetime. Format YYYY-MM-DDTHH:MM:SS±HH:MM. Example: 2017-02-11T08:00:00-04:00.
   (S/optional-key :open_tracking) S/Bool ; Whether open tracking is enabled for this transmission
   (S/optional-key :click_tracking) S/Bool ; Whether click tracking is enabled for this transmission
   (S/optional-key :transactional) S/Bool ; Whether message is transactional for unsubscribe and suppression purposes. Note no List-Unsubscribe header is included in transactional messages.
   (S/optional-key :sandbox) S/Bool ; Whether to use the sandbox sending domain
   (S/optional-key :skip_suppression) S/Bool ; Whether to ignore customer suppression rules, for this transmission only.
   (S/optional-key :ip_pool) S/Str ; The ID of a dedicated IP pool associated with your account. If this field is not provided, the account’s default dedicated IP pool is used  (if there are IPs assigned to it).
   (S/optional-key :inline_css) S/Bool ; Whether to perform CSS inlining in HTML content. Note only rules in head > style elements will be inlined.
   })

(def Transmission
  {(S/optional-key :options) Options ; JSON object in which transmission options are defined
   :recipients [Recipient] ; Inline recipient objects or object containing stored recipient list ID
   (S/optional-key :campaign_id) S/Str ; Name of the campaign
   (S/optional-key :description) S/Str ; Description of the transmission
   (S/optional-key :metadata) {S/Keyword S/Any} ; Key/value pairs that are provided to the substitution engine
   (S/optional-key :substitution_data) {S/Keyword S/Any} ; Key/value pairs that are provided to the substitution engine
   (S/optional-key :return_path) S/Str ; Email address to use for envelope FROM
   :content Content ; Content that will be used to construct a message
   })

(def TransmissionSuccess
  {:results
   {:total_rejected_recipients S/Int
    :total_accepted_recipients S/Int
    :id S/Str
    S/Keyword S/Any}
   S/Keyword S/Any})

(def TransmissionFailure
  {:errors [{:description S/Str
             :code S/Str
             :message S/Str
             S/Keyword S/Any}]
   S/Keyword S/Any})

(defn- api-post-request
  [{{api-key :api-key
     endpoint :endpoint} :config}
   method 
   params]
  {:url (str endpoint method)
   :method :post
   :headers {"Content-Type" "application/json; charset=utf-8"
             "Authorization" api-key}
   :body (json/encode params)})

(def ^:private coerce-t-s 
  (coercer TransmissionSuccess json-coercion-matcher))

(def ^:private coerce-t-f
  (coercer TransmissionFailure json-coercion-matcher))

(defn- check-schema-error
  [obj]
  (if-let [error (error-val obj)]
    (ex-info (str "Schema error: " (pr-str error))
             error)
    obj))

(def ^:private xform-transmission-response
  (http-client/xform-response
    {:200 (fn [{:keys [body]}] 
            (-> (json/decode body true)
                (coerce-t-s)
                (check-schema-error)))
     :400 (fn [{:keys [body]}] 
            (let [result (-> (json/decode body true)
                             (coerce-t-f)
                             (check-schema-error))]
              (if (error? result)
                result
                (ex-info (str "Transmission failed: " (:message (first (:errors result))))
                         result))))}))

(defn- transmission-response-chan
  []
  (async/chan 1 xform-transmission-response))

(S/defn transmission! 
  "Send a transmision"
  [{:keys [config] :as this}
   params :- Transmission]
  (with-resources this [:http]
    (let [params (if (:sandbox? config)
                   (assoc-in params [:options :sandbox] true)
                   params)
          req (api-post-request this "/v1/transmissions" params)]
      (http-client/request! http req (transmission-response-chan)))))

(defrecord SparkPostClient [config])

(S/defn create
  [resource-name
   config :- Config]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->SparkPostClient
        {:config config})
      resource-name nil
      [(http-client/create :http (:http config))])))
