(ns web-corpus.relevant-content-and-samples
  "Sample generation codebase"
  (:require [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [subotai.timestamps :as ts]
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

(defn get-warcs
  [job-dir]
  (filter
   (fn [f]
     (and
      (re-find
       #".*.warc.gz"
       (.getAbsolutePath f))))
   (file-seq
    (io/as-file job-dir))))

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

(defn in-clueweb12pp-range?
  [d]
  (t/within?
   (t/interval (t/date-time 2012 2)
               (t/date-time 2012 5))
   d))

(defn warc-clueweb-time-stats
  [stream]
  (let [records 
        (filter
         (fn [r]
           (and (= (:warc-type r)
                   "response")
                (< 1000
                   (-> r
                       :content-length
                       Integer/parseInt))))
         (warc/stream-warc-records-seq stream))

        to-return 
        (reduce
         (fn [acc r]
           (let [doc-dates (-> r :payload ts/document-detected-timestamps)]
             (if (some (fn [d] (in-clueweb12pp-range? d))
                       doc-dates)
               (inc acc)
               acc)))
         0
         records)]
    to-return))

(defn clueweb-time-stats
  "How many documents
   that are in the Clueweb12++ time range"
  [job-dir]
  (let [warcs (get-warcs job-dir)]
    (reduce
     (fn [acc w]
       (let [stream (warc/warc-input-stream w)
             to-return 
             (+ acc (warc-clueweb-time-stats stream))]
         (.close stream)
         to-return))
     0
     warcs)))
