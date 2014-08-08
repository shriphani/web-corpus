(ns web-corpus.detect-index-page
  "Module to detect an index page. In particular
   we try to locate a set of records (loosely)
   that are sorted in descending order"
  (:require [clj-time.core :as t]
            [web-corpus.utils :as utils]
            [subotai.timestamps :as ts])
  (:import [com.google.common.base CharMatcher]))

(defn too-few-chars?
  [t]
  (<= (count t)
      5))

(defn index-page-times
  "We expect a set of [node date] pairs"
  [nodes-timestamps]
  (reduce
   (fn [chain [node date]]
     (cond (empty? chain)
           [date]
           
           (t/after? (last chain)
                     date)
           (concat chain [date])

           :else
           []))
   nodes-timestamps))

(defn index-page?
  "Checks for a trail of at least
   10 timestamps in descending order.

   This is a nice painless number"
  [page-src]
  (let [page-timestamps (ts/timestamps-and-nodes page-src)

        paths-timestamps 
        (group-by
         (fn [[n d]]
           (utils/node-path n))
         (map
          (fn [[n d]]
            [n (first d)])
          (filter
           (fn [[n d]]
             (->> n
                  (.getTextContent)
                  (.trimFrom CharMatcher/WHITESPACE)
                  too-few-chars?
                  not))
           page-timestamps)))

        large-enough-groups (filter
                             (fn [[p ts]]
                               (< 10 (count ts)))
                             paths-timestamps)]
    (first
     (filter
      (fn [[g ns]]
        (<= 10 (count (index-page-times ns))))
      large-enough-groups))))
