(ns web-corpus.mine-next-thread
  "From the to-sample list, we mine the
   next thread type links for a brand new
   Heritrix job. Woo !"
  (:require [clojure.java.io :as io])
  (:import [java.io PushbackReader]))

(defn to-sample-records
  [rdr]
  (take-while
   identity
   (repeatedly
    (fn []
      (try (read rdr)
           (catch Exception e nil))))))

(defn process-to-sample-file
  [to-sample-file]
  (let [rdr (-> to-sample-file
                io/reader
                PushbackReader.)

        records (to-sample-records rdr)]
    
    (doseq [a-sample records]
      (doseq [{uri :uri
               path :path
               links :links}
              a-sample]
        (doseq [[text link] links]
          (when (re-find #"Next Thread" text)
            (println link)))))))
