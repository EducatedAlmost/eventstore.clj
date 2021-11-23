(ns ae.eventstore
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            ;; [clj-uuid :as uuid]
            [java-time :as time]
            ;; [clojure.pprint :as pp]
            [clojure.core.match :as match]
            [ae.alias+ns :as ns])
  (:import [java.util.concurrent TimeUnit]
           [com.eventstore.dbclient
            ;; ,   Acls
            ,   AppendToStreamOptions
            ,   ClusterInfo
            ,   ConnectionMetadata
            ,   ConnectionSettingsBuilder
            ,   ConnectionShutdownException
            ,   Consts
            ,   ConsumerStrategy
            ,   CreateContinuousProjectionOptions
            ,   CreateOneTimeProjectionOptions
            ,   CreatePersistentSubscriptionOptions
            ;; ,   CreatePersistentSubscriptionToAllOptions
            ,   DeletePersistentSubscriptionOptions
            ,   DeleteResult
            ,   DeleteStreamOptions
            ,   Direction
            ,   Endpoint
            ,   EventData
            ,   EventDataBuilder
            ,   EventFilter
            ,   EventStoreDBClient
            ,   EventStoreDBClientSettings
            ,   EventTypeFilter
            ,   ExpectedRevision
            ,   NodePreference
            ;; ,   NodeSelector
            ,   ParseError
            ,   PersistentSubscriptionSettings
            ,   PersistentSubscriptionSettingsBuilder
            ;; ,   PersistentSubscriptionToAllSettings
            ;; ,   PersistentSubscriptionToAllSettingsBuilder
            ,   Position
            ,   ReadAllOptions
            ,   ReadResult
            ,   ReadStreamOptions
            ,   RecordedEvent
            ,   RegularFilterExpression
            ,   ResolvedEvent
            ,   SpecialStreamRevision
            ;; ,   StreamAcl
            ,   StreamFilter
            ;; ,   StreamMetadata
            ,   StreamNotFoundException
            ,   StreamRevision
            ,   SubscribePersistentSubscriptionOptions
            ,   SubscribeToAllOptions
            ,   SubscribeToStreamOptions
            ,   SubscriptionFilter
            ,   SubscriptionFilterBuilder
            ,   SubscriptionListener
            ,   Timeouts
            ,   TimeoutsBuilder
            ;; ,   UpdatePersistentSubscriptionOptions
            ;; ,   UpdatePersistentSubscriptionToAllOptions
            ,   UserCredentials
            ,   WriteResult
            ,   WrongExpectedVersionException]))

;; Aliases

(ns/aliases+ns
 {'cred            'ae.eventstore.credentials
  'direction       'ae.eventstore.direction
  'ep              'ae.eventstore.endpoint
  'event           'ae.eventstore.event
  'event.data      'ae.eventstore.event.data
  'event.data.type 'ae.eventstore.event.data.type
  'np              'ae.eventstore.node-preference
  'options         'ae.eventstore.options
  'position        'ae.eventstore.position
  'revision        'ae.eventstore.revision
  'resolved        'ae.eventstore.resolved
  'settings        'ae.eventstore.settings
  'stream          'ae.eventstore.stream
  'sub             'ae.eventstore.subscription})

;; Specs

(do
  (s/def ::event/id uuid?)
  (s/def ::event/type string?)
  (s/def ::event/data any?)
  (s/def ::event.data/type #{::event.data.type/json ::event.data.type/octet-stream})
  (s/def ::user.metadata any?)
  (s/def ::stream/id string?)
  (s/def ::revision/stream (s/or :int int? :enum #{::revision/start ::revision/end}))
  (s/def ::revision/expected
    (s/or :specific int? :enum #{::revision/any ::revision/stream-exists ::revision/no-stream}))
  (s/def ::position any?)
  (s/def ::created
    (s/with-gen #(instance? java.time.Instant %) (fn [] (gen/fmap time/instant (s/gen int?)))))

  (s/def ::event
    (s/keys :req [::event.id ::event.type ::event.data ::event.data.type ::user.metadata]))
  (s/def ::recorded
    (s/keys :req [::stream.id ::revision/stream ::event.id ::position
                  ::system.metadata ::event.data ::user.metadata]))
  (s/def ::resolved/event ::recorded)
  (s/def ::resolved/link ::recorded)
  (s/def ::resolved (s/keys :req [::resolved/event ::resolved/link]))

  (s/def ::sub/on-event (s/fspec :args (s/cat :sub ::subscription :event ::resolved)))
  (s/def ::sub/on-error (s/fspec :args (s/cat :sub ::subscription :error ::throwable)))
  (s/def ::sub/on-cancelled (s/fspec :args (s/cat :sub ::subscription)))
  (s/def ::listener (s/keys :req [::sub/on-event ::sub/on-error ::sub/on-cancelled]))

  (s/def ::cred/username string?)
  (s/def ::cred/password string?)
  (s/def ::credentials (s/keys :req [::cred/username ::cred/password]))

  (s/def ::node-preference #{::np/leader ::np/follower ::np/read-only-replica ::np/random})

  (s/def ::ep/hostname string?)
  (s/def ::ep/port int?)
  (s/def ::endpoint (s/keys :req [::ep/hostname ::ep/port]))

  (s/def ::settings/dns-discover? boolean?)
  (s/def ::settings/max-discover-attempts int?)
  (s/def ::settings/gossip-timeout int?)
  (s/def ::settings/tls? boolean?)
  (s/def ::settings/tls-verify-cert? boolean?)
  (s/def ::settings/throw-on-append-failure? boolean?)
  (s/def ::settings/hosts (s/coll-of ::endpoint))
  (s/def ::settings/keep-alive-timeout int?)
  (s/def ::settings/keep-alive-interval int?)
  (s/def ::settings
    (s/keys :req [::settings/dns-discover?
                  ::settings/max-discover-attempts
                  ::settings/discovery-interval
                  ::settings/gossip-timeout
                  ::node-preference
                  ::settings/tls?
                  ::settings/tls-verify-cert?
                  ::settings/throw-on-append-failure?
                  ::credentials
                  ::settings/hosts
                  ::settings/keep-alive-timeout
                  ::settings/keep-alive-interval]))

  (s/def ::options/timeouts
    (s/keys :req [::options/shutdown-timeout ::options/shutdown-timeout-unit
                  ::options/subscription-timeout ::options/subscription-timeout-unit]))

  (s/def ::options
    (s/keys :req [::options/timeouts
                  ::credentials
                  ::options/requires-leader?
                  ::options/resolve-link-tos?
                  ::revision/expected
                  ::options/soft-delete?
                  ::revision/stream
                  ::position
                  ::direction
                  ::sub/filter])))

;; Import functions

(defn ->ContentType [t]
  (match/match t
    ::event.data.type/json "application/json"
    ::event.data.type/octet-stream "application/octet-stream"))

(defn ->EventData [{:keys [::event/id ::event/data ::user-metadata]
                    event-type ::event/type content-type ::event.data/type}]
  ;; TODO use the builder to create the byte array for data
  (new EventData id event-type content-type data user-metadata))

(defn ->StreamRevision [sr]
  (match/match sr
    ::revision/start (StreamRevision/START)
    ::revision/end (StreamRevision/END)
    :else (new StreamRevision sr)))

(defn ->ExpectedRevision [er]
  (match/match er
    ::revision/any (ExpectedRevision/ANY)
    ::revision/no-stream (ExpectedRevision/NO_STREAM)
    ::revision/stream-exists (ExpectedRevision/STREAM_EXISTS)
    :else (ExpectedRevision/expectedRevision er)))

(defn ->Position [{::position/keys [commit prepare]}]
  (new Position commit prepare))

(defn ->RecordedEvent [{stream-id ::stream/id stream-revision ::revision/stream
                        event-id ::event/id event-type ::event/type event-data
                        ::event/data content-type ::event.data/type
                        ::keys [user-metadata created position]}]
  (new RecordedEvent
       stream-id (->StreamRevision stream-revision) event-id (->Position position)
       {"content-type" (->ContentType content-type)
        "created" (str created)
        "is-json" (= content-type ::event.data.type/json)
        "type" event-type}
       event-data user-metadata))

(defn ->ResolvedEvent [{:keys [::resolved/event ::resolved/link]}]
  (new ResolvedEvent event link))

(defn ->Listener [{::sub/keys [on-event on-error on-cancelled]}]
  (proxy [SubscriptionListener] []
    (onEvent [sub event] (on-event sub event))
    (onError [sub error] (on-error sub error))
    (onCancelled [sub] (on-cancelled sub))))

(defn ->UserCredentials [{::cred/keys [username password]}]
  (new UserCredentials username password))

(defn ->NodePreference [np]
  (match/match np
    ::np/leader            (NodePreference/LEADER)
    ::np/follower          (NodePreference/FOLLOWER)
    ::np/read-only-replica (NodePreference/READ_ONLY_REPLICA)
    ::np/random            (NodePreference/RANDOM)))

(defn ->Endpoint [{::ep/keys [hostname port]}]
  (new Endpoint hostname port))

(defn add-host [builder host]
  (.addHost builder (->Endpoint host)))

(defn ->Settings [{::keys [node-preference]
                   {::cred/keys [username password]} ::credentials
                   ::settings/keys [dns-discover?
                                    max-discover-attempts
                                    discovery-interval
                                    gossip-timeout
                                    tls?
                                    tls-verify-cert?
                                    throw-on-append-failure?
                                    hosts
                                    keep-alive-timeout
                                    keep-alive-interval]}]
  (cond-> (new ConnectionSettingsBuilder)
    (some? dns-discover?)            (.dnsDiscover dns-discover?)
    (some? max-discover-attempts)    (.maxDiscoverAttempts max-discover-attempts)
    (some? discovery-interval)       (.discoveryInterval discovery-interval)
    (some? gossip-timeout)           (.gossipTimeout gossip-timeout)
    (some? node-preference)          (.nodePreference (->NodePreference node-preference))
    (some? tls?)                     (.tls tls?)
    (some? tls-verify-cert?)         (.tlsVerifyCert tls-verify-cert?)
    (some? throw-on-append-failure?) (.throwOnAppendFailure throw-on-append-failure?)
    (and  (some? username)
          (some? password))          (.defaultCredentials username password)
    (some? keep-alive-timeout)       (.keepAliveTimeout keep-alive-timeout)
    (some? keep-alive-interval)      (.keepAliveInterval keep-alive-interval)
    (some? hosts)                    #(reduce add-host % hosts)
    true                             (.buildConnectionSettings)))

(defn ->TimeUnit [unit]
  (match/match unit
    :days         (TimeUnit/DAYS)
    :hours        (TimeUnit/HOURS)
    :minutes      (TimeUnit/MINUTES)
    :seconds      (TimeUnit/SECONDS)
    :milliseconds (TimeUnit/MILLISECONDS)
    :microseconds (TimeUnit/MICROSECONDS)
    :nanoseconds  (TimeUnit/NANOSECONDS)
    :else (-> "Cannot be converted to TimeUnit: %s"
              (format (str unit)) (Exception.) throw)))

(defn ->Timeouts [{::options/keys [shutdown-timeout shutdown-timeout-unit
                                   subscription-timeout subscription-timeout-unit]}]
  (let [b (TimeoutsBuilder/newBuilder)]
    (cond-> b
      (and shutdown-timeout shutdown-timeout-unit)
      ,   (.withShutdownTimeout shutdown-timeout (->TimeUnit shutdown-timeout-unit))
      (and subscription-timeout subscription-timeout-unit)
      ,   (.withSubscriptionTimeout subscription-timeout (->TimeUnit subscription-timeout-unit))
      true (.build))))

(defn apply-base-options [builder {::options/keys [timeouts requires-leader?] ::keys [credentials]}]
  (cond-> builder
    (some? timeouts)         (.Timeouts (->Timeouts timeouts))
    (some? requires-leader?) (.requiresLeader requires-leader?)
    (some? credentials)      (.authenticated (->UserCredentials credentials))
    true                     (.build)))

(defn ->AppendOptions [{stream-revision ::revision/stream :as options}]
  (cond-> (-> AppendToStreamOptions .get (apply-base-options options))
    (some? stream-revision) (.expectedRevision (->ExpectedRevision stream-revision))
    true                    (.build)))

(defn ->ReadStreamOptions [{:keys [::direction ::options/resolve-link-tos?]
                            stream-revision ::revision/stream :as options}]
  (cond-> (-> ReadStreamOptions .get (apply-base-options options))
    (= direction ::direction/forwards)  (.forwards)
    (= direction ::direction/backwards) (.backwards)
    (some? resolve-link-tos?)           (.resolveLinkTos resolve-link-tos?)
    (some? stream-revision)             (.fromRevision (->StreamRevision stream-revision))
    true                                (.build)))

(defn ->ReadAllOptions [{:keys [::direction ::options/resolve-link-tos? ::position] :as options}]
  (cond-> (-> ReadAllOptions .get (apply-base-options options))
    (= direction ::direction/forwards)  (.forwards)
    (= direction ::direction/backwards) (.backwards)
    (some? resolve-link-tos?)           (.resolveLinkTos resolve-link-tos?)
    (some? position)                    (.fromPosition (->Position position))
    true                                (.build)))

;; Export functions

(defn ContentType-> [t]
  (match/match t
    "application/json"         ::event.data.type/json
    "application/octet-stream" ::event.data.type/octet-stream))

(defn NodePreference-> [np]
  (match/match (.name np)
    "LEADER"            ::np/leader
    "FOLLOWER"          ::np/follower
    "READ_ONLY_REPLICA" ::np/read-only-replica
    "RANDOM"            ::np/random))

(defn Endpoint-> [e]
  {::endpoint/hostname (.getHostname e)
   ::endpoint/port (.getPort e)})

;; Methods

(defn connect [settings]
  (EventStoreDBClient/create (->Settings settings)))

(defn shutdown [client]
  (.shutdown client))

(defn ->future [f]
  (future (.get f)))

(defn append [client stream options events]
  (-> client (.appendToStream stream (->AppendOptions options) (map ->EventData events)) ->future))

(defn set-stream-meta [client stream append-options]
  (-> client (.setStreamMetadata stream (->AppendOptions append-options)) ->future))

(defn read-stream [client stream max-count read-options]
  (-> client (.readStream stream max-count (->ReadStreamOptions read-options)) ->future))

(defn get-stream-meta [client stream read-options]
  (-> client (.getStreamMetadata stream (->ReadStreamOptions read-options)) ->future))

(defn read-all [client max-count read-options]
  (-> client (.readAll max-count (->ReadAllOptions read-options)) ->future))
