(defproject web_corpus "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "0.9.2"]
                 [clj-time "0.8.0"]
                 [com.github.kyleburton/clj-xpath "1.4.3"]
                 [com.google.guava/guava "17.0"]
                 [incanter "1.5.5"]
                 [org.bovinegenius/exploding-fish "0.3.4"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [subotai "0.2.12"]]
  :main web-corpus.main)
