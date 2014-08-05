(ns web-corpus.process-kba-links
  "Take kba permalinks crawl and produce a set of 1 hop
   explorations or a set of 1 hop samples"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [subotai.warc.warc :as warc]
            [web-corpus.thread-page :as tp])
  (:use [clojure.pprint :only [pprint]]))

(defn handle-warc
  [indices-file warc-file]
  (let [indices     (map
                     #(Integer/parseInt %)
                     (string/split-lines
                      (slurp indices-file)))
        warc-stream (warc/warc-input-stream warc-file)
        warc-records (warc/stream-warc-records-seq warc-stream)
        indexed-records (map vector (iterate inc 0) warc-records)]
    (doseq [index indices]
      (let [record (second
                    (first
                     (drop-while
                      (fn [[i r]]
                        (not= i index))
                      indexed-records)))]
        (pprint
         (tp/generate-links-to-sample (:payload record)
                                      (:warc-target-uri record)))))))

(defn -main
  [& args]
  (let [indices-file (first args)
        to-dump-file (second args)
        warc-file (nth args 2)]
    (let [wrtr (io/writer to-dump-file
                          :append
                          true)]
      (binding [*out* wrtr]
        (handle-warc indices-file
                     warc-file)))))
