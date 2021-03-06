(ns web-corpus.main
  "Tie it all together bro"
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [web-corpus.kba-permalinks :as kba-permalinks]
            [web-corpus.mine-next-thread :as mine-next-thread]
            [web-corpus.mine-next-thread-indices :as mine-next-thread-indices]
            [web-corpus.next-thread-extractor :as next-thread-extractor]
            [web-corpus.process-kba-links :as process-kba-links]
            [web-corpus.relevant-content-and-samples :as samples]
            [web-corpus.common-crawl-corpus :as common-crawl-corpus]
            [web-corpus.sampling :as sampling])
  (:use [clojure.pprint :only [pprint]]
        [incanter core stats charts io])
  (:import [java.io File]))

(def options
  [[nil "--next-thread-seeds F" "A sample"]
   [nil "--next-thread-extractor J" "Extract next thread from job dir j"]
   [nil "--kba-permalinks W" "Run permalinks on warc"]
   [nil "--process-kba-links W" "Process indices file"]
   [nil "--out-file F" "Dump results to"]
   [nil "--warc-file W" "Warc file"]
   [nil "--samples J" "Job dir samples"]
   [nil "--stats J" "Job dir stats"]
   [nil "--time-stats J" "Job dir clueweb related stats"]
   [nil "--common-crawl-corpus C" "Process the common crawl corpus"]
   [nil "--ignore-hosts F" "Generate a list of processed hosts"]
   [nil "--next-thread-seeds-2 F" "Supply a warc file, expect indices to be built bro"]])

(defn -main
  [& args]
  (let [{options :options} (parse-opts args options)]
    (cond (:next-thread-seeds options)
          (let [out-file (string/replace (:next-thread-seeds options)
                                         #"to_sample_data.clj"
                                         "next_thread_seeds.txt")
                out-wrtr (io/writer out-file :append true)]
            (binding [*out* out-wrtr]
              (-> options
                  :next-thread-seeds
                  mine-next-thread/process-to-sample-file)
              (.close out-wrtr)))
          
          (:next-thread-extractor options)
          (let [job-dir  (:next-thread-extractor options)
                out-file "foo.seeds"
                out-wrtr (io/writer out-file)]
            (do (binding [*out* out-wrtr]
                  (next-thread-extractor/process-job job-dir))
                (.close out-wrtr)
                (let [rdr (io/reader out-file)
                      wtr (io/writer (str job-dir
                                          "action/foo.seeds"))]
                  (io/copy rdr wtr)
                  (.close rdr)
                  (.close wtr))))

          (:kba-permalinks options)
          (kba-permalinks/handle-corpus
           (:kba-permalinks options)
           (:out-file options))

          (:process-kba-links options)
          (process-kba-links/-main (:process-kba-links options)
                                   (:out-file options)
                                   (:warc-file options))

          (:samples options)
          (samples/generate-sample (:samples options))

          (:stats options)
          (do (spit (:out-file options)
                 (str (c/to-long
                       (t/now))
                      ","
                      (samples/stats (:stats options))
                      ","))
              (let [data (read-dataset (:out-file options))
                    dates (sel data :cols 0)
                    cnt (sel data :cols 1)]
                (save (time-series-plot dates cnt :y-label "Downloaded")
                      (string/replace (:out-file options)
                                      #".csv$"
                                      ".png"))))
          
          (:time-stats options)
          (do (spit (:out-file options)
                 (str (c/to-long
                       (t/now))
                      ","
                      (samples/clueweb-time-stats (:time-stats options))
                      ","))
              (let [data (read-dataset (:out-file options))
                    dates (sel data :cols 0)
                    cnt (sel data :cols 1)]
                (save (time-series-plot dates cnt :y-label "In Clueweb range")
                      (string/replace (:out-file options)
                                      #".csv$"
                                      ".png"))))
          
          (:common-crawl-corpus options)
          (let [out-file (string/replace
                          (:common-crawl-corpus options)
                          #".corpus$"
                          ".uris")
                out-handle (io/writer out-file)]
            (binding [*out* out-handle]
              (common-crawl-corpus/process-common-crawl-corpus
               (:common-crawl-corpus options))))

          (:ignore-hosts options)
          (let [seeds-file (:ignore-hosts options)]
            (sampling/build-handled-seeds seeds-file))

          (:next-thread-seeds-2 options)
          (let [out-file (str (:next-thread-seeds-2 options) ".next-thread-seeds")
                out-wrtr (io/writer out-file :append true)]
            (binding [*out* out-wrtr]
              (mine-next-thread-indices/handle-warc-file
               (:next-thread-seeds-2 options)))))))
