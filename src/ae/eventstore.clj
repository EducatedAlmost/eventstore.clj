(ns ae.eventstore
  (:require [clojure.spec.alpha :as s]
            [clojure.core.match :as match])
  (:import [com.eventstore.dbclient
            , Endpoint
            , EventStoreDBClient
            , NodePreference
            , UserCredentials]))

;; Settings

(create-ns 'ae.eventstore.credentials)
(alias 'cred 'ae.eventstore.credentials)

(s/def ::cred/username string?)
(s/def ::cred/password string?)

(s/def ::credentials (s/keys :req [::cred/username ::cred/password]))

(defn ->UserCredentials [{::cred/keys [username password]}]
  (UserCredentials. username password))

(defn UserCredentials-> [_] nil)

(create-ns 'ae.eventstore.node-preference)
(alias 'np 'ae.eventstore.node-preference)

(s/def ::node-preference
  #{::np/leader
    ::np/follower
    ::np/read-only-replica
    ::np/random})

(defn ->NodePreference [np]
  (match/match np
    ::np/leader            (NodePreference/LEADER)
    ::np/follower          (NodePreference/FOLLOWER)
    ::np/read-only-replica (NodePreference/READ_ONLY_REPLICA)
    ::np/random            (NodePreference/RANDOM)))

(defn NodePreference-> [np]
  (match/match (.name np)
    "LEADER"            ::np/leader
    "FOLLOWER"          ::np/follower
    "READ_ONLY_REPLICA" ::np/read-only-replica
    "RANDOM"            ::np/random))

(create-ns 'ae.eventstore.endpoint)
(alias 'endpoint 'ae.eventstore.endpoint)

(s/def ::endpoint/hostname string?)
(s/def ::endpoint/port int?)

(s/def ::endpoint
  (s/keys :req [::endpoint/hostname ::endpoint/port]))

(defn ->Endpoint [{::endpoint/keys [hostname port]}]
  (Endpoint. hostname port))

(defn Endpoint-> [e]
  {::endpoint/hostname (.getHostname e)
   ::endpoint/port (.getPort e)})

(create-ns 'ae.eventstore.settings)
(alias 'sett 'ae.eventstore.settings)

(s/def ::sett/dns-discover? boolean?)
(s/def ::sett/max-discover-attempts int?)
(s/def ::sett/gossip-timeout int?)
(s/def ::sett/tls? boolean?)
(s/def ::sett/tls-verify-cert? boolean?)
(s/def ::sett/throw-on-append-failure? boolean?)
(s/def ::sett/hosts (s/coll-of ::endpoint))
(s/def ::sett/keep-alive-timeout int?)
(s/def ::sett/keep-alive-interval int?)

(s/def ::settings
  (s/keys :req [::sett/dns-discover?
                ::sett/max-discover-attempts
                ::sett/discovery-interval
                ::sett/gossip-timeout
                ::node-preference
                ::sett/tls?
                ::sett/tls-verify-cert?
                ::sett/throw-on-append-failure?
                ::credentials
                ::sett/hosts
                ::sett/keep-alive-timeout
                ::sett/keep-alive-interval]))

(defn ->Settings [s] nil)

(s/def ::db-client
  (s/keys :req [::grpc-client ::credentials]))

(defn connect [settings]
  (.EventStoreDBClient (->Settings settings)))

;; Event

(create-ns 'ae.eventstore.event)
(alias 'event 'ae.eventstore.event)

(create-ns 'ae.eventstore.event.data)
(alias 'e.data 'ae.eventstore.event.data)

(create-ns 'ae.eventstore.event.data.type)
(alias 'e.d.type 'ae.eventstore.event.data.type)

(s/def ::event/id uuid?)
(s/def ::event/type any?)
(s/def ::e.data/value any?)
(s/def ::e.data/type #{::e.d.type/json ::e.d.type/binary})
(s/def ::event/data (s/keys :req [::event/id ::e.data/value ::e.data/type]))
(s/def ::event/metadata ::event/data)
