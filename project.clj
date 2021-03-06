(defproject org.clojars.lambdatronic/geo-xls "1.0.0-SNAPSHOT"
  :description "geo-xls is a simple command-line script which converts
                a properly formatted XLS file (see README), describing
                Workspaces, Data/Coverage Stores, and Layers into the
                Geoserver REST commands to create those objects in a
                running Geoserver instance."
  :dependencies     [[org.clojure/clojure "1.2.0"]
                     [org.clojure/clojure-contrib "1.2.0"]
                     [dk.ative/docjure "1.4.0"]]
  :dev-dependencies [[clojure-source "1.2.0"]]
  :main             geo-xls.core)

;; For more options to defproject, see:
;;   https://github.com/technomancy/leiningen/blob/master/sample.project.clj
