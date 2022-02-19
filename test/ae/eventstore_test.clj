(ns ae.eventstore-test
  (:require
   [ae.eventstore :as sut]
   [clojure.test :as t]
   [clojure.spec.test.alpha :as stest]
   [clojure.spec.test.check :as-alias stc]
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.alpha :as s]
   [cheshire.core :as json]
   [clj-uuid :as uuid]
   [clojure.java.data :as j]
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
    [subscription :as-alias sub]]))

(defn spectest [sym]
  (t/testing (str "spectest:" sym)
    (t/is (-> (stest/check sym {::stc/opts {:num-tests 100}})
              first ::stc/ret :pass?))))

;; No generator for fn?
#_(stest/check `sut/update-some {::stc/opts {:num-tests 10}})

(t/deftest update-some-test
  (t/testing "Missing keys will not be created."
    (let [m {} k :foo f +]
      (t/is (= m (sut/update-some m k f)))))
  (t/testing "Present keys will be updated."
    (let [m {:foo 1} k :foo f inc n {:foo 2}]
      (t/is (= n (sut/update-some m k f))))))

(t/deftest str->ba-test
  (t/testing "str->ba spec."
    (t/is (-> (stest/check `sut/str->ba) first ::stc/ret :pass?))))

(t/deftest Instant->Ticks-test
  (spectest `sut/Instant->Ticks))

(t/deftest ->StreamRevision-test
  (spectest `sut/->StreamRevision))

(t/deftest StreamRevision->-test
  (spectest `sut/StreamRevision->))

(t/deftest ->Position-test
  (spectest `sut/->Position))

(t/deftest Position->-test
  (spectest `sut/->Position))

(t/deftest ->Direction-test
  (spectest `sut/->Direction))

(t/deftest Direction->-test
  (spectest `sut/Direction->))

(t/deftest ->Data-test
  (t/testing "Objects are converted to JSON."
    (let [obj {:a "foo"}
          expected "{\"a\": \"foo\"}"
          actual (sut/->Data obj)]
      (t/is expected actual))))

(t/deftest ->EventData-test
  (t/testing ""
    (let [id (uuid/v4)
          type "e-type"
          data {:foo "bar"}
          u-meta {:tag 1}
          event {:ae.eventstore.event/id id
                 :ae.eventstore.event/type type
                 :ae.eventstore.event/data data
                 :ae.eventstore.metadata/user u-meta}
          expected (-> (com.eventstore.dbclient.EventDataBuilder/json id type data)
                       (.metadataAsJson u-meta)
                       .build)
          actual (sut/->EventData event)]
      (t/is (= (j/from-java expected) (j/from-java actual))))))

(t/deftest EventData->-test
  (t/testing ""
    (let [id (uuid/v4)
          type "e-type"
          data {:foo "bar"}
          u-meta {:tag 1}
          expected {:ae.eventstore.event/id id
                    :ae.eventstore.event/type type
                    :ae.eventstore.event/data data
                    :ae.eventstore.metadata/user u-meta}
          actual (-> expected sut/->EventData sut/EventData->)]
      (t/is (= expected actual)))))

(t/deftest ->RecordedEvent-test
  (t/testing ""
    (let [id (uuid/v4)
          type "e-type"
          instant (time/instant)
          stream-id "s-id"
          data {:foo "bar"}
          u-meta {:tag 1}
          e {::event/id id
             ::event/type type
             ::event/data data
             ::sut/created instant
             ::sut/position {::pos/commit -1 ::pos/prepare -1}
             ::stream/id stream-id
             ::rev/stream ::rev/end
             ::metadata/user u-meta}
          expected (new com.eventstore.dbclient.RecordedEvent stream-id
                        (com.eventstore.dbclient.StreamRevision/END)
                        id
                        (com.eventstore.dbclient.Position/END)
                        {"content-type" "application/json"
                         "created" (str (sut/Instant->Ticks instant))
                         "is-json" "true"
                         "type" type}
                        (sut/->Data data)
                        (sut/->Data u-meta))
          actual (sut/->RecordedEvent e)]
      (t/is (= (j/from-java expected)
               (j/from-java actual))))))

(t/deftest RecordedEvent->-test
  (t/testing ""
    (let [id (uuid/v4)
          type "e-type"
          instant (time/instant)
          stream-id "s-id"
          data {:foo "bar"}
          u-meta {:tag 1}
          expected {::event/id id
                    ::event/type type
                    ::event/data data
                    ::sut/created instant
                    ::sut/position {::pos/commit -1 ::pos/prepare -1}
                    ::stream/id stream-id
                    ::rev/stream ::rev/end
                    ::metadata/user u-meta}
          actual (sut/RecordedEvent-> (sut/->RecordedEvent expected))]
      (t/is (= (dissoc expected ::sut/created)
               (dissoc actual ::sut/created))))))
