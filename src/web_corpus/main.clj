(ns web-corpus.main
  "Tie it all together bro"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [web-corpus.mine-next-thread :as mine-next-thread]
            [web-corpus.next-thread-extractor :as next-thread-extractor]))

(def options
  [[nil "--next-thread-seeds F" "A sample"]
   [nil "--next-thread-extractor J" "Extract next thread from job dir j"]])

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
          (let [job-dir (:next-thread-extractor options)
                out-file (str job-dir "action/foo.seeds")
                out-wrtr (io/writer out-file :append true)]
            (do (binding [*out* out-wrtr]
                  (next-thread-extractor/process-job job-dir))
                (.close out-wrtr))))))
