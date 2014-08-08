(ns web-corpus.common-crawl-corpus
  "Process the corpus files from the
   common crawl"
  (:require [clojure.java.io :as io]
            [web-corpus.detect-index-page :as detect-index-page])
  (:import [java.io PushbackReader]))

(defn get-records-seq
  [rdr]
  (take-while
   identity
   (repeatedly
    (fn []
      (try (read rdr)
           (catch Exception e nil))))))

(defn process-common-crawl-corpus
  [a-corpus-file]
  (let [rdr (-> a-corpus-file
                io/reader
                PushbackReader.)]
    (doseq [{url  :url
             body :body}
            (get-records-seq rdr)]
      (if (detect-index-page/index-page? body)
        (println url)))))
