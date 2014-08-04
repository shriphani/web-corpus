(ns web-corpus.kba-permalinks
  "Handle kba permalinks"
  (:require [clj-time.core :as t]
            [clojure.java.io :as io]
            [subotai.warc.warc :as warc])
  (:use [subotai.timestamps]))

(defn get-warc-files
  "A list of paths to warc files"
  [job-dir]
  (map
   #(.getAbsolutePath %)
   (filter
    (fn [f]
      (and (re-find #".warc.gz" (.getAbsolutePath f))
           (not
            (re-find #"latest" (.getAbsolutePath f)))))
    (file-seq
     (io/as-file job-dir)))))

(defn in-kba-range?
  [a-page]
  (let [dates (document-detected-timestamps a-page)]
    (some
     (fn [a-date]
       (t/within?
        (t/interval (t/date-time 2012 7)
                    (t/date-time 2012 8))
        a-date))
     dates)))

(defn handle-warc-file
  [a-warc-file]
  (let [warc-reader (warc/warc-input-stream a-warc-file)
        warcs       (warc/stream-warc-records-seq warc-reader)
        indexed-warcs (map vector (iterate inc 0) warcs)]
    (map
     first
     (filter
      (fn [[i w]]
        (and (= (:warc-type w)
                "response")
             (-> w :payload in-kba-range?)))
      indexed-warcs))))

(defn handle-corpus
  [warc out-file]
  (let [indices (handle-warc-file warc)
        out-handle (io/writer out-file)]
    (doseq [index indices]
      (binding [*out* out-handle]
        (println index)))))
