(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'ae/ae.eventstore.j)
(def version "0.0.1")
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file "target/ae-eventstore-j.jar")

(defn clean [_]
  (b/delete {:path "target"}))

(defn aot-compile [_]
  (b/javac {:src-dirs ["java"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["-source" "8" "-target" "8"]}))

(defn aot-compile-2 [_]
  (aot-compile nil)
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir}))

(defn jar [_]
  (aot-compile nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))
