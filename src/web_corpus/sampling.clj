(ns web-corpus.sampling
  "for each seed, create a list of links
   that could be potential sampling candidates"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [org.bovinegenius [exploding-fish :as uri]])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.io PushbackReader]))

(defn build-handled-seeds
  [seeds-txt-file]
  (let [wrtr (io/writer "ignore-hosts")
        hosts
        (distinct
         (map
          uri/host
          (string/split-lines
           (slurp seeds-txt-file))))]
    (doseq [host hosts]
      (binding [*out* wrtr]
        (println host)))
    (.close wrtr)))

(def to-ignore-hosts (set
                      (string/split-lines
                       (slurp "ignore-hosts"))))

(defn not-handled-hosts
  [rdr]
  (let [records (take-while
                 identity
                 (repeatedly
                  (fn []
                    (try (read rdr)
                         (catch Exception e nil)))))]
    (filter
     (fn [r]
       (not
        (some
         #{(uri/host
            (:uri
             (first r)))}
         to-ignore-hosts)))
     records)))
