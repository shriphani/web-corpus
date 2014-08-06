(ns web-corpus.relevant-content-and-samples
  "Sample generation codebase"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [subotai.warc.warc :as warc]
            [web-corpus.utils :as utils]))

(defn get-latest-warc
  [job-dir]
  (first
   (filter
    (fn [f]
      (and
       (re-find
        #"latest.*.warc.gz.open"
        (.getAbsolutePath f))))
    (file-seq
     (io/as-file job-dir)))))

(defn get-crawl-logs
  [job-dir]
  (filter
   (fn [f]
     (re-find #"crawl.log"
              (.getAbsolutePath f)))
   (file-seq
     (io/as-file job-dir))))

(defn generate-sample
  [job-dir]
  (let [latest-warc (get-latest-warc job-dir)
        warc-stream (warc/warc-input-stream latest-warc)
        content-records (filter
                         (fn [r]
                           (and (= (:warc-type r)
                                   "response")
                                (< 1000
                                   (-> r
                                       :content-length
                                       Integer/parseInt))))
                         (warc/stream-warc-records-seq warc-stream))]
    (doseq [[r i]
            (map
             vector
             (take
              10
              (filter
               identity
               (filter
                (fn [r]
                  (if (zero? (rand-int 2))
                    r
                    nil))
                content-records)))
             (range 10))]
      (spit (str "/bos/www/htdocs/spalakod/web_corpus/" i "-sample.html")
            (:payload r)))))


(defn stats
  [job-dir]
  (let [crawl-logs (get-crawl-logs job-dir)]
    (reduce
     (fn [acc l]
       (let [rdr (io/reader l)
             lines (line-seq rdr)

             new-acc
             (+ acc
                (reduce
                 (fn [acc-file line]
                   (let [parts (string/split line #"\s+")
                         code  (Integer/parseInt (nth parts 1))
                         link  (nth parts 3)]
                     (if (and (= 200 code)
                              (not
                               (re-find #"robots.txt" link))
                              (not
                               (re-find #"dns:" link)))
                       (+ acc-file 1)
                       acc-file)))
                 0
                 lines))]
         (do (.close rdr)
             new-acc)))
     0
     crawl-logs)))
