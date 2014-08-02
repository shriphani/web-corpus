(ns web-corpus.detect-thread-page
  "Thread page classifier"
  (:use [subotai.timestamps]))

(defn process-page
  [page-src]
  (timestamps-and-nodes page-src))
