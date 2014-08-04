(ns web-corpus.thread-page
  "Codebase to deal with a thread page
   by producing a sample and detecting links
   to the neighborhood"
  (:require [subotai.representation :as representation]
            [web-corpus.utils :as utils])
  (:use [clj-xpath.core :only [$x:node*]]))

(defn links-on-page
  [a-page uri]
  (let [xml-doc (representation/html->xml-doc a-page)
        nodes-anchors (utils/anchor-nodes a-page uri)]
    (reduce
     (fn [acc [n h]]
       (let [path (utils/node-path n)]
         (merge-with concat
                     acc
                     {path [[n h]]})))
     {}
     nodes-anchors)))

(defn generate-links-to-sample
  [a-page uri]
  (let [grouped-links (links-on-page a-page uri)]
    (map
     (fn [[path nodes]]
       (let [to-sample (utils/random-take (if (< 10 (count nodes))
                                            (quot (count nodes) 4)
                                            (count nodes))
                                          nodes)]
         {:uri uri
          :path  path
          :links (map
                  (fn [[n h]]
                    [(.getTextContent n) h])
                  to-sample)}))
     grouped-links)))

