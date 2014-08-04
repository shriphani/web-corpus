(ns web-corpus.check-kba-links
  "Test out what the kba links look like"
  (:require [clojure.string :as string]
            [subotai.warc.warc :as warc]))

(defn handle-warc-file
  [index-file warc-file]
  (let [warc-stream (warc/warc-input-stream warc-file)
        warc-records (warc/stream-warc-records-seq warc-stream)
        indices (map
                 #(Integer/parseInt %)
                 (string/split-lines
                  (slurp index-file)))]
    (doseq [i indices]
      (println
       (:warc-target-uri
        (nth warc-records i))))))

(defn get-kba-stuff
  "An index file contains record
   indices"
  [an-index-file]
  (let [associated-warc (string/replace an-index-file #".thread-indices" "")]
    (handle-warc-file an-index-file
                      associated-warc)))
