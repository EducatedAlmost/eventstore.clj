(ns ae.eventstore
  (:require
   [ae.alias+ns :as ns]
   [cheshire.core :as json]
   [clj-uuid :as uuid]
   [clojure.core.match :as match]
   [clojure.java.data :as j]
   [clojure.java.data.builder :as builder]
   [clojure.math.numeric-tower :as math]
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen]
   [clojure.string :as str]
   [java-time :as time]
   [ae.eventstore
    [content-type :as-alias ct]
    [credentials :as-alias cred]
    [direction :as-alias direction]
    [endpoint :as-alias ep]
    [event :as-alias event]
    [metadata :as-alias metadata]
    [node-preference :as-alias np]
    [options :as-alias options]
    [position :as-alias pos]
    [revision :as-alias rev]
    [resolved :as-alias resolved]
    [result :as-alias result]
    [settings :as-alias settings]
    [stream :as-alias stream]
    [subscription :as-alias sub]])
  (:import
   (ae.eventstore ReadAllOptionsCljTwo)
   (ae.eventstore.j ReadAllOptionsClj)
   (com.eventstore.dbclient
    ,   AppendToStreamOptions
        ,   ClusterInfo
        ,   ConnectionMetadata
        ,   ConnectionSettingsBuilder
        ,   ConnectionShutdownException
        ,   Consts
        ,   ConsumerStrategy
        ,   CreatePersistentSubscriptionOptions
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
        ,   EventStoreDBConnectionString
        ,   EventTypeFilter
        ,   ExpectedRevision
        ,   NodePreference
        ,   ParseError
        ,   PersistentSubscriptionSettings
        ,   PersistentSubscriptionSettingsBuilder
        ,   Position
        ,   ReadAllOptions
        ,   ReadResult
        ,   ReadStreamOptions
        ,   RecordedEvent
        ,   RegularFilterExpression
        ,   ResolvedEvent
        ,   SpecialStreamRevision
        ,   StreamFilter
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
        ,   UserCredentials
        ,   WriteResult
        ,   WrongExpectedVersionException)
   (java.util.concurrent TimeUnit)))

(do
  (s/def ::event/id uuid?)
  (s/def ::event/type string?)
  (s/def ::stream/id string?)
  (s/def ::event/data any?) ;; NB this will be decoded
  (s/def ::event/data-raw any?)

  (s/def ::content-type #{::ct/json ::ct/octet-stream})
  (s/def ::created (s/with-gen #(instance? java.time.Instant %) (fn [] (gen/fmap time/instant (s/gen int?)))))
  (s/def ::json? boolean?)

  (s/def ::rev/stream (s/or :int int? :enum #{::rev/start ::rev/end}))

  (s/def ::rev/expected
    (s/or :specific int? :enum #{::rev/any ::rev/stream-exists ::rev/no-stream})) ;; actually unrelated to revision/stream

  (s/def ::pos/prepare int?)
  (s/def ::pos/commit int?)
  (s/def ::position
    (s/with-gen (s/keys :req [::pos/prepare ::pos/commit])
      (fn [] (gen/fmap #(identity {::pos/commit (+ % (rand-int 10)) ::pos/prepare %})
                       (s/gen (s/and int? #(> % -1)))))))

  (s/def ::metadata/user (s/keys :req []))
  (s/def ::metadata/system (s/keys :req [::content-type ::created ::json? ::event/type]))

  (s/def ::event
    (s/keys :req [::event/id ::event/type
                  ::event/data ::event/data-raw
                  ::content-type ::metadata/user]))

  (s/def ::recorded
    (s/merge ::event
             (s/keys :req [::stream/id ::rev/stream
                           ::metadata/user ::metadata/system
                           ::position ::created])))

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

  (s/def ::direction #{::direction/forwards ::direction/backwards})

  (s/def ::ep/hostname string?)
  (s/def ::port (s/and int? pos? #(< % 65535)))
  (s/def ::ep/port ::port)
  (s/def ::endpoint (s/keys :req [::ep/hostname ::ep/port]))

  (s/def ::settings/dns-discover? boolean?)
  (s/def ::settings/max-discover-attempts int?)
  (s/def ::settings/discovery-interval  int?)
  (s/def ::settings/gossip-timeout int?)
  (s/def ::settings/tls? boolean?)
  (s/def ::settings/tls-verify-cert? boolean?)
  (s/def ::settings/throw-on-append-failure? boolean?)
  (s/def ::settings/hosts (s/coll-of ::endpoint))
  (s/def ::settings/keep-alive-timeout (s/and int? #(> % 10000)))
  (s/def ::settings/keep-alive-interval (s/and int? #(> % 10000)))
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

  (s/def ::timeunit #{:days :hours :minutes :seconds :milliseconds :microseconds :nanoseconds})
  (s/def ::long? (s/and int? #(< % (math/expt 2 63)) #(> % (- (math/expt 2 63)))))
  (s/def ::options/shutdown-timeout ::long?)
  (s/def ::options/shutdown-timeout-unit ::timeunit)
  (s/def ::options/subscription-timeout ::long?)
  (s/def ::options/subscription-timeout-unit ::timeunit)

  (s/def ::options/timeouts
    (s/keys :req [::options/shutdown-timeout ::options/shutdown-timeout-unit
                  ::options/subscription-timeout ::options/subscription-timeout-unit]))

  (s/def ::options/requires-leader? boolean?)
  (s/def ::options/resolve-link-tos? boolean?)
  (s/def ::options/soft-delete? boolean?)
  (s/def ::options/requires-leader? boolean?)

  (s/def ::options/base
    (s/keys :opt [::options/timeouts ::credentials ::options/requires-leader?]))

  (s/def ::options/with-resolve-link-tos
    (s/merge ::options/base (s/keys :opt [::options/resolve-link-tos?])))

  (s/def ::options/with-expected-revision
    (s/merge ::options/base (s/keys :opt [::rev/expected])))

  (s/def ::options/with-pos-and-resolve
    (s/merge ::options/with-resolve-link-tos (s/keys :opt [::position])))

  (s/def ::options/with-rev-and-resolve
    (s/merge ::options/with-resolve-link-tos (s/keys :opt [::rev/stream])))

  (s/def ::options/append-to-stream ::options/with-expected-revision)
  (s/def ::options/read-stream
    (s/merge ::options/with-rev-and-resolve (s/keys :opt [::direction])))
  (s/def ::options/read-all
    (s/merge ::options/with-pos-and-resolve (s/keys :opt [::direction])))
  (s/def ::options/delete-stream
    (s/merge ::options/with-expected-revision (s/keys :opt [::options/soft-delete?])))

  (s/def ::result/write (s/keys :req [::rev/stream ::position]))
  (s/def ::result/read (s/coll-of ::resolved)))

;; Generators
;; (gen/sample (s/gen ::port) 100)

(def keymap->
  {:eventId ::event/id
   :streamId ::stream/id
   :eventType ::event/type
   :streamRevision ::rev/stream
   :created ::created
   :contentType ::content-type
   :userMetadata ::metadata/user
   :commitUnsigned ::pos/commit
   :prepareUnsigned ::pos/prepare
   :position ::position
   :eventData ::event/data
   :hostname ::ep/hostname
   :port ::ep/port
   :endpoint ::endpoint
   :hosts ::settings/hosts
   :nodePreference ::node-preference
   :credentials ::credentials
   :dnsDiscover ::settings/dns-discover?
   :maxDiscoverAttempts ::settings/max-discover-attempts
   :discoveryInterval ::settings/discovery-interval
   :gossipTimeout ::settings/gossip-timeout
   :tls ::settings/tls?
   :tlsVerifyCert ::settings/tls-verify-cert?
   :throwOnAppendFailure ::settings/throw-on-append-failure?
   :keepAliveTimeout ::settings/keep-alive-timeout
   :keepAliveInterval ::settings/keep-alive-interval})

(def ->keymap (set/map-invert keymap->))

(defn update-some [m k f]
  (if (contains? m k) (update m k f) m))

(defn str->ba [s]
  (->> s
       (map byte)
       byte-array))

(defn x->ba [x json?]
  (-> x ((if json? json/generate-string str)) str->ba))

(defn Instant->Ticks [i]
  (let [secs (.getEpochSecond i)
        nanos (.getNano i)
        ticks (+ (/ nanos 100.0) (* secs (math/expt 10 7)))]
    (long ticks)))

(defn ->uuid [u]
  (uuid/v4 (:mostSignificantBits u) (:leastSignificantBits u)))

(defn ->ContentType ^String [t]
  (match/match t
    ::ct/json "application/json"
    ::ct/octet-stream "application/octet-stream"))

(defn ContentType-> [ct]
  (match/match ct
    "application/json" ::ct/json
    "application/octet-stream" ::ct/octet-stream))

(defn ->StreamRevision [sr]
  (match/match sr
    ::rev/start (StreamRevision/START)
    ::rev/end (StreamRevision/END)
    :else (new StreamRevision sr)))

(defn -StreamRevision-> [sr]
  (let [n (:valueUnsigned sr)]
    (match/match n
      -1 ::rev/end
      0 ::rev/start
      :else n)))

(defn StreamRevision-> [sr]
  (-> sr j/from-java -StreamRevision->))

(defn ->ExpectedRevision [er]
  (match/match er
    ::rev/any (ExpectedRevision/ANY)
    ::rev/no-stream (ExpectedRevision/NO_STREAM)
    ::rev/stream-exists (ExpectedRevision/STREAM_EXISTS)
    :else (ExpectedRevision/expectedRevision er)))

(defn ExpectedRevision-> [er] nil)

(defn ->Position [{::pos/keys [commit prepare]}]
  (new Position commit prepare))

(defn -Position-> [p]
  (set/rename-keys p keymap->))

(defn Position-> [p]
  (-> p j/from-java -Position->))

(defn ->Direction [d]
  (match/match d
    ::direction/forwards (Direction/Forwards)
    ::direction/backwards (Direction/Backwards)))

(defn Direction-> [d] nil)

(defn ->Data [d ct]
  (if (= ct ::ct/json) d
      (if (= (class d) (class (byte-array []))) d
          (->> d (map byte) byte-array))))

(defn Data-> [d ct]
  (-> d byte-array String. ((if (= ct "application/json") #(json/parse-string % true) identity))))

(defn ->EventData [{:keys [::event/type ::event/id ::event/data ::content-type ::metadata/user] :as event}]
  (let [props (set/rename-keys event ->keymap)]
    (if (= ::ct/octet-stream content-type)
      (builder/to-java EventData (EventDataBuilder/binary type (->Data data content-type))
                       (merge props {:metadataAsBytes (->Data user content-type)}) {})
      (builder/to-java EventData (EventDataBuilder/json type data)
                       (merge props {:metadataAsJson user}) {}))))

(defn EventData-> [e]
  (let [event (-> e j/from-java (set/rename-keys keymap->))
        ct (::content-type event)]
    (-> event
        (update-some ::event/id ->uuid)
        (update-some ::content-type ContentType->)
        (update-some ::event/data #(Data-> % ct))
        (update-some ::metadata/user #(Data-> % ct)))))

(-> {::event/id (uuid/v4)
     ::event/type "type"
     ::event/data "data"
     ::content-type ::ct/octet-stream}
    ->EventData
    EventData->)

(defn ->RecordedEvent
  [{:keys [::event/id ::event/type ::event/data
           ::content-type ::created ::position]
    stream-id ::stream/id stream-revision ::rev/stream  user-metadata ::metadata/user
    :as event}]
  (new RecordedEvent stream-id (->StreamRevision stream-revision) id (->Position position)
       {"content-type" (->ContentType content-type)
        "created" (str (Instant->Ticks created))
        "is-json" (= content-type ::ct/json)
        "type" type}
       (x->ba data (= content-type ::ct/json))
       (x->ba user-metadata (= content-type ::ct/json))))

(defn RecordedEvent-> [re]
  (let [x (-> re j/from-java (set/rename-keys keymap->))
        ct (::content-type x)]
    (-> x
        (update ::event/id ->uuid)
        (update ::event/data #(Data-> % ct))
        (update ::metadata/user #(Data-> % ct))
        (update ::position -Position->)
        (update ::content-type ContentType->)
        (update ::rev/stream -StreamRevision->)
        (update ::created #(java.time.Instant/ofEpochSecond (:epochSecond %) (:nano %))))))

(-> {::event/id #uuid "12a91086-5de8-4bce-89b2-54274e191165"
     ::event/type "type" ::event/data {:foo [:bar]}
     ::rev/stream ::rev/end ::content-type ::ct/json ::created (time/instant)
     ::position {::pos/commit 4 ::pos/prepare 3} ::stream/id "stream"
     ::metadata/user ["flummox"]}
    ->RecordedEvent
    RecordedEvent->)

(defn ->ResolvedEvent [{:keys [::resolved/event ::resolved/link]}]
  (new ResolvedEvent (->RecordedEvent event) (->RecordedEvent link)))

(defn ResolvedEvent-> [re]
  {::resolved/event (-> re .getEvent RecordedEvent->)
   ::resolved/link (-> re .getLink RecordedEvent->)})

(-> {::event/id #uuid "12a91086-5de8-4bce-89b2-54274e191165"
     ::event/type "type" ::event/data {:foo [:bar]}
     ::rev/stream ::rev/end ::content-type ::ct/json ::created (time/instant)
     ::position {::pos/commit 4 ::pos/prepare 3} ::stream/id "stream"
     ::metadata/user ["flummox"]}
    (#(assoc {} ::resolved/event % ::resolved/link %))
    ->ResolvedEvent
    ResolvedEvent->)

(defn ->Listener [{::sub/keys [on-event on-error on-cancelled]}]
  (proxy [SubscriptionListener] []
    (onEvent [sub event] (on-event sub event))
    (onError [sub error] (on-error sub error))
    (onCancelled [sub] (on-cancelled sub))))

;; Settings

(defn ->UserCredentials [{::cred/keys [username password]}]
  (new UserCredentials username password))

(defn ->NodePreference [np]
  (match/match np
    ::np/leader            (NodePreference/LEADER)
    ::np/follower          (NodePreference/FOLLOWER)
    ::np/read-only-replica (NodePreference/READ_ONLY_REPLICA)
    ::np/random            (NodePreference/RANDOM)))

(defn -NodePreference-> [np]
  (match/match np
    "LEADER" ::np/leader
    "FOLLOWER" ::np/follower
    "READ_ONLY_REPLICA" ::np/read-only-replica
    "RANDOM" ::np/random))

(defn NodePreference-> [np]
  (condp = np
    (NodePreference/LEADER)            ::np/leader
    (NodePreference/FOLLOWER)          ::np/follower
    (NodePreference/READ_ONLY_REPLICA) ::np/read-only-replica
    (NodePreference/RANDOM)            ::np/random))

(-> ::np/follower
    ->NodePreference
    NodePreference->)

(defn ->Endpoint [{::ep/keys [hostname port]}]
  (new Endpoint hostname port))

(defn -Endpoint-> [ep]
  (set/rename-keys ep keymap->))

(defn Endpoint-> [ep]
  (-> ep j/from-java (set/rename-keys keymap->)))

(-> {::ep/hostname "goole" ::ep/port 54444443}
    ->Endpoint
    Endpoint->)

(defn add-host [^ConnectionSettingsBuilder builder host]
  (.addHost builder (->Endpoint host)))

(defn String->Settings [str]
  (EventStoreDBConnectionString/parse str))

(defn ->Settings ^EventStoreDBClientSettings [s]
  (if (string? s) (String->Settings s)
      (let [hosts (::settings/hosts s)
            {:keys [::cred/username ::cred/password]} (::credentials s)
            builder (-> (EventStoreDBClientSettings/builder)
                        (#(reduce add-host % hosts))
                        (#(if (and (some? username) (some? password))
                            (.defaultCredentials % username password) %)))
            props (-> s
                      (update-some ::node-preference ->NodePreference)
                      (assoc :foo :bar)
                      (set/rename-keys ->keymap))]
        (builder/to-java EventStoreDBClientSettings ConnectionSettingsBuilder builder
                         props {:build-fn "buildConnectionSettings"}))))

(defn Settings->
  ([s] (Settings-> s nil))
  ([s credentials]
   (let [settings (-> s j/from-java (set/rename-keys keymap->))]
     (-> settings
         (dissoc :defaultCredentials)
         (#(if (some? credentials) (assoc % ::credentials credentials) %))
         (update ::node-preference -NodePreference->)
         (update ::settings/hosts #(map -Endpoint-> %))))))

(-> {:ae.eventstore.settings/discovery-interval 12633835,
     :ae.eventstore.settings/max-discover-attempts 57508,
     :ae.eventstore.settings/hosts [#:ae.eventstore.endpoint{:hostname "9VkCWl", :port 19142}
                                    #:ae.eventstore.endpoint{:hostname "kjbgm30WlO8B70NEqyr71u2", :port 683}
                                    #:ae.eventstore.endpoint{:hostname "R0B1418Cj0327", :port 4159}
                                    #:ae.eventstore.endpoint{:hostname "Ab1T2iY7b6vej5ng5", :port 46007}
                                    #:ae.eventstore.endpoint{:hostname "5iELy25", :port 119}
                                    #:ae.eventstore.endpoint{:hostname "Hx7X2iBQ8X25pEa", :port 5}
                                    #:ae.eventstore.endpoint{:hostname "R0AT9", :port 816}
                                    #:ae.eventstore.endpoint{:hostname "jA", :port 5}],
     :ae.eventstore.settings/tls? true,
     :ae.eventstore.settings/gossip-timeout 67,
     :ae.eventstore/credentials #:ae.eventstore.credentials{:username "JrjegVEN4fg0d", :password ""},
     :ae.eventstore.settings/throw-on-append-failure? false,
     :ae.eventstore.settings/keep-alive-interval 214569,
     :ae.eventstore.settings/keep-alive-timeout 18672969,
     :ae.eventstore/node-preference :ae.eventstore.node-preference/leader,
     :ae.eventstore.settings/tls-verify-cert? true,
     :ae.eventstore.settings/dns-discover? false}
    ->Settings
    (Settings-> (gen/generate (s/gen ::credentials))))

;; Options

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

(defn TimeUnit-> [unit]
  (-> unit j/from-java str/lower-case keyword))

(-> :seconds ->TimeUnit TimeUnit->)

(defn ->Timeouts
  ([] (Timeouts/DEFAULT))
  ([{::options/keys [shutdown-timeout shutdown-timeout-unit
                     subscription-timeout subscription-timeout-unit]}]
   (let [b (TimeoutsBuilder/newBuilder)]
     (cond-> b
       (and shutdown-timeout shutdown-timeout-unit)
       ,   (.withShutdownTimeout shutdown-timeout (->TimeUnit shutdown-timeout-unit))
       (and subscription-timeout subscription-timeout-unit)
       ,   (.withSubscriptionTimeout subscription-timeout (->TimeUnit subscription-timeout-unit))
       true (.build)))))

(defn Timeouts-> [_]
  (throw (new Exception "This method is unimplemented due to limitations of the underlying library.")))

(-> (gen/generate (s/gen ::options/timeouts))
    ->Timeouts)

;; TODO convert stream revision into expected revision

;; Base
;; Timeouts
;; Credentials
;; RequiresLeader
(defn apply-base-options
  [b {:keys [::options/timeouts ::credentials ::options/requires-leader?]}]
  (cond-> b
    (some? timeouts) (.timeouts (->Timeouts timeouts))
    (some? credentials) (.authenticated (->UserCredentials credentials))
    (some? requires-leader?) (.requiresLeader requires-leader?)))

;; ExpectedRevision
;; Base
(defn ->AppendToStreamOptions
  ([] (AppendToStreamOptions/get))
  ([{:keys [::rev/expected ::rev/stream] :as o}]
   (cond-> (-> (AppendToStreamOptions/get) (apply-base-options o))
     (or (some? expected) (some? stream)) (.expectedRevision (->ExpectedRevision (or expected stream))))))

;; Direction
;; StreamRevision
;; ResolveLinkTos
;; Base
;;
(defn forwards [o]
  (.forwards o))

(defn backwards [o]
  (.backwards o))

(defn ->ReadStreamOptions
  ([] (ReadStreamOptions/get))
  ([{:keys [::rev/stream ::direction ::options/resolve-link-tos?] :as o}]
   (cond-> (-> (ReadStreamOptions/get) (apply-base-options o))
     (some? direction) ((if (= direction ::direction/forwards) forwards backwards))
     (some? stream) (.fromRevision (->StreamRevision stream))
     (some? resolve-link-tos?) (.resolveLinkTos resolve-link-tos?))))

;; Direction
;; Position
;; ResolveLinkTos
;; Base
(defn ->ReadAllOptions
  ([] (ReadAllOptions/get))
  ([{:keys [::position ::direction ::options/resolve-link-tos?] :as o}]
   (cond-> (-> (ReadAllOptions/get) (apply-base-options o))
     (some? direction) ((if (= direction ::direction/forwards) forwards backwards))
     (some? position) (.fromPosition (->Position position))
     (some? resolve-link-tos?) (.resolveLinkTos resolve-link-tos?))))

(-> (gen/generate (s/gen ::options/read-all))
    ->ReadAllOptions)

;; SoftDelete
;; ExpectedRevision
;; Base
;;
(defn soft-delete [o]
  (.softDelete o))

(defn hard-delete [o]
  (.hardDelete o))

(defn ->DeleteStreamOptions
  ([] (DeleteStreamOptions/get))
  ([{:keys [::options/soft-delete? ::rev/expected ::rev/stream] :as o}]
   (cond-> (-> (DeleteStreamOptions/get) (apply-base-options o))
     (some? soft-delete?) ((if soft-delete? soft-delete hard-delete))
     (some? (or expected stream)) (.expectedRevision (->ExpectedRevision (or expected stream))))))

;; Results

;; WriteResult
(defn ->WriteResult [{:keys [::rev/stream ::position]}]
  (new WriteResult (->StreamRevision stream) (->Position position)))

(defn WriteResult-> [wr]
  (-> wr j/from-java
      (set/rename-keys {:logPosition ::position :nextExpectedRevision ::rev/stream})
      (update ::position -Position->)
      (update ::rev/stream -StreamRevision->)))

(-> {:ae.eventstore.revision/stream -1447132, :ae.eventstore/position #:ae.eventstore.position{:prepare -10622, :commit -1}}
    ->WriteResult
    WriteResult->
    (#(s/valid? ::result/write %)))

;; ReadResult
(defn ->ReadResult [rr]
  (new ReadResult (map ->ResolvedEvent rr)))

(defn ReadResult-> [rr]
  (->> rr .getEvents (map ResolvedEvent->)))

(-> (gen/generate (s/gen ::result/read))
    ->ReadResult
    ReadResult->)

;; Subscriptions

;; Methods

(defn connect [settings]
  (EventStoreDBClient/create (->Settings settings)))

(defn shutdown [client]
  (.shutdown client))

(defn ->future [f]
  (future (.get f)))

(defn append [client stream options events]
  (-> client (.appendToStream stream (->AppendToStreamOptions options) (map ->EventData events)) ->future))

(defn set-stream-meta [client stream append-options]
  (-> client (.setStreamMetadata stream (->AppendToStreamOptions append-options)) ->future))

(defn read-stream [client stream max-count read-options]
  (-> client (.readStream stream max-count (->ReadStreamOptions read-options)) ->future))

(defn get-stream-meta [client stream read-options]
  (-> client (.getStreamMetadata stream (->ReadStreamOptions read-options)) ->future))

(defn read-all [client max-count read-options]
  (-> client
      (.readAll max-count (->ReadAllOptions read-options))
      ->future))
