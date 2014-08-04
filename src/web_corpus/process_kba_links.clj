(ns web-corpus.process-kba-links
  "Take kba permalinks crawl and produce a set of 1 hop
   explorations or a set of 1 hop samples"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [subotai.warc.warc :as warc]
            [web-corpus.thread-page :as tp])
  (:use [clojure.pprint :only [pprint]]))

(defn handle-warc
  [indices-file]
  (let [indices     (map
                     #(Integer/parseInt %)
                     (string/split-lines
                      (slurp indices-file)))
        warc-file   (string/replace indices-file #".thread-indices" "")
        warc-stream (warc/warc-input-stream warc-file)
        warc-records (warc/stream-warc-records-seq warc-stream)]
    (doseq [index indices]
      (let [record (nth warc-records index)]
        (pprint
         (tp/generate-links-to-sample (:payload record)
                                      (:warc-target-uri record)))))))

(defn -main
  [& args]
  (let [indices-file (first args)
        to-dump-dir  (second args)]
    (let [wrtr (io/writer (str to-dump-dir
                               "to_sample_data.clj")
                          :append
                          true)]
      (binding [*out* wrtr]
        (handle-warc indices-file)))))
