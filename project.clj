(defproject org.clojars.lambdatronic/geo-xls "1.0.0-rc2"
  :description "geo-xls is a simple command-line script which converts
                a properly formatted XLS file (see README), describing
                Workspaces, Data/Coverage Stores, and Layers into the
                Geoserver REST commands to create those objects in a
                running Geoserver instance."
  :dependencies     [[org.clojure/clojure "1.2.0"]
                     [org.clojure/clojure-contrib "1.2.0"]
                     [dk.ative/docjure "1.4.0"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [clojure-source "1.2.0"]
                     [lein-clojars "0.6.0"]]
  :jvm-opts         ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :main             geo-xls.core)

;; For more options to defproject, see:
;;   https://github.com/technomancy/leiningen/blob/master/sample.project.clj
