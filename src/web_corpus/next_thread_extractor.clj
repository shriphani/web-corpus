(ns web-corpus.next-thread-extractor
  "Next thread in the pivot jobs"
  (:require [clojure.java.io :as io]
            [subotai.warc.warc :as warc]
            [web-corpus.utils :as utils])
  (:use [clojure.pprint :only [pprint]]))

;; a map of warcs in a job dir that have been addressed
(def handled-warcs
  (try (read-string
        (slurp "handled-warcs.clj"))
       (catch Exception e {})))

(defn job-warcs
  [job-dir]
  (filter
   (fn [f]
     (and (re-find #".warc.gz" f)
          (not
           (re-find #"latest" f))))
   (map
    #(.getAbsolutePath %)
    (file-seq
     (io/as-file job-dir)))))

(defn handle-warc-file
  [warc]
  (let [warc-stream (warc/warc-input-stream warc)
        records-seq (warc/stream-warc-records-seq warc-stream)

        response-records 
        (filter
         (fn [x]
           (= (:warc-type x)
              "response"))
         records-seq)]
    (doseq [record response-records]
      (let [record-anchors (utils/anchor-nodes (:payload record)
                                               (:warc-target-uri record))]
        (doseq [[n l] record-anchors]
          (when (or (re-find #"Next Thread" (.getTextContent n))
                    (re-find #"Previous Thread" (.getTextContent n)))
            (println l)))))))

(defn process-job
  [job-dir]
  (let [warcs (job-warcs job-dir)
        processed (set (handled-warcs job-dir))

        not-processed
        (filter
         (fn [w]
           (not
            (some #{w} processed)))
         warcs)

        full-not-processed
        (filter
         (fn [w]
           (re-find #".warc.gz$" w))
         not-processed)]

    (do (doall
         (map
          (fn [w]
            (handle-warc-file w))
          not-processed))
        (pprint (merge-with concat handled-warcs {job-dir full-not-processed})
                (io/writer "handled-warcs.clj")))))
