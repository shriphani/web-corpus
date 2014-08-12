(ns web-corpus.mine-next-thread-indices
  "Mine next thread directly from
   the file indices"
  (:require [clojure.string :as string]
            [org.bovinegenius [exploding-fish :as uri]]
            [subotai.warc.warc :as warc]
            [web-corpus.utils :as utils]))

(def hosts-to-ignore (set
                      (string/split-lines
                       (slurp "ignore-hosts"))))

(defn operate-on-threads
  [warc-file indices-file]
  (let [indices (set
                 (map
                  #(Integer/parseInt %)
                  (string/split-lines
                   (slurp indices-file))))

        stream (warc/warc-input-stream warc-file)
        records (filter
                 (fn [x]
                   (= (:warc-type x)
                      "response"))
                 (warc/stream-warc-records-seq stream))

        indexed-records (map vector
                             (iterate inc 0)
                             records)]
    (doall
     (doseq [[i r] indexed-records]
       (when (and (some #{i} indices)
                  (not
                   (some
                    #{(uri/host (:warc-target-uri r))}
                    hosts-to-ignore)))
         ;(println (:warc-target-uri r))
         (let [anchors (try (utils/anchor-nodes (:payload r)
                                                (:warc-target-uri r))
                            (catch Exception e []))]
           
           (doseq [[n h] anchors]
             (when (or (re-find #"Next Thread" (.getTextContent n))
                       (re-find #"Previous Thread" (.getTextContent n))
                       (re-find #"previous" (.getTextContent n))
                       (re-find #"next" (.getTextContent n)))
               (println h)
               (flush)))))))))

(defn handle-warc-file
  [warc-file]
  (let [indices-file (str warc-file ".thread-indices")]
    (operate-on-threads warc-file
                        indices-file)))
