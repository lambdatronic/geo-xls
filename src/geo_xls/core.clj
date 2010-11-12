;;; geo-xls - Copyright 2010 Gary W. Johnson (gwjohnso@uvm.edu)
;;;
;;; Description:
;;;
;;;   geo-xls is a simple command-line script which converts a
;;;   properly formatted XLS file (see README), describing Workspaces,
;;;   Data/Coverage Stores, and Layers into the Geoserver REST
;;;   commands to create those objects in a running Geoserver
;;;   instance.

(ns geo-xls.core
  (:use dk.ative.docjure.spreadsheet)
  (:use clojure.contrib.prxml)
  (:use [clojure.contrib.http.agent :rename {bytes http-agent-bytes}])
  (:use clojure.contrib.base64)
  (:use clojure.java.shell))

(def *spreadsheet-filename* "/home/gjohnson/school/phd/Geoserver_REST_database.xls")

(def *raster-column-spec*
     {:A :Workspace
      :B :Store
      :C :Layer
      :D :Description
      :E :URI
      :F :DefaultStyle
      :J :NativeSRS
      :K :DeclaredSRS})

(def *vector-column-spec*
     {:A :Workspace
      :B :Store
      :C :Layer
      :E :Description
      :F :URI
      :G :DefaultStyle
      :K :NativeSRS
      :L :DeclaredSRS})

(defn load-column-data
  [spreadsheet-filename sheet-name column-spec]
  (->> (load-workbook spreadsheet-filename)
       (select-sheet sheet-name)
       (select-columns column-spec)
       next))

(def *raster-data* (load-column-data *spreadsheet-filename*
                                     "Raster"
                                     *raster-column-spec*))

(def *vector-data* (load-column-data *spreadsheet-filename*
                                     "Vector"
                                     *vector-column-spec*))

(def *geoserver-rest-uri*     "http://ecoinformatics.uvm.edu/geoserver/rest")
(def *geoserver-username*     "admin")
(def *geoserver-password*     "rnbh304")
(def *geoserver-auth-code*    (str "Basic " (encode-str (str *geoserver-username* ":" *geoserver-password*))))
(def *geoserver-data-dir*     "/raid/geoserver/")
(def *aries-namespace-prefix* "http://www.integratedmodelling.org/geo/ns/")

(defn get-store-type
  [uri]
  (condp re-matches uri
    #"^file:.*\.tif$"    "GeoTIFF"
    #"^file:.*\.shp$"    "Shapefile"
    #"^postgis:.*\.shp$" "PostGIS Table"
    #"^postgis:.*$"      "PostGIS Database"))

(defn form-rest-uri
  [uri-suffix]
  (str *geoserver-rest-uri* uri-suffix))

(defn form-namespace
  [Workspace]
  (str *aries-namespace-prefix* Workspace))

(defn create-workspace-and-namespace
  [{:keys [Workspace]}]
  ;;(println "create-workspace-and-namespace" Workspace)
  [(form-rest-uri "/namespaces")
   :method  "POST"
   :headers {"Accepts"       "application/xml",
             "Content-type"  "application/xml",
             "Authorization" *geoserver-auth-code*}
   :body    (with-out-str (prxml [:namespace
                                  [:prefix Workspace]
                                  [:uri (form-namespace Workspace)]]))])

(defn extract-dbname
  [uri]
  (second (re-find #"^postgis:(.*)$" uri)))

(defn create-postgis-data-store
  [{:keys [Workspace Store Description URI]}]
  ;;(println "create-postgis-data-store" (str Workspace ":" Store))
  [(form-rest-uri (str "/workspaces/" Workspace "/datastores"))
   :method  "POST"
   :headers {"Accepts"       "application/xml",
             "Content-type"  "application/xml",
             "Authorization" *geoserver-auth-code*}
   :body    (with-out-str (prxml [:dataStore
                                  [:name Store]
                                  [:description Description]
                                  [:type "PostGIS"]
                                  [:enabled "true"]
                                  [:connectionParameters
                                   [:entry {:key "host"}                         "localhost"]
                                   [:entry {:key "port"}                         "5432"]
                                   [:entry {:key "dbtype"}                       "postgis"]
                                   [:entry {:key "database"}                     (extract-dbname URI)]
                                   [:entry {:key "user"}                         "postgres"]

                                   [:entry {:key "namespace"}                    (form-namespace Workspace)]
                                   [:entry {:key "schema"}                       "public"]

                                   [:entry {:key "min connections"}              "1"]
                                   [:entry {:key "max connections"}              "10"]
                                   [:entry {:key "validate connections"}         "true"]
                                   [:entry {:key "Connection timeout"}           "20"]

                                   [:entry {:key "fetch size"}                   "1000"]
                                   [:entry {:key "Loose bbox"}                   "false"]
                                   [:entry {:key "Expose primary keys"}          "false"]
                                   [:entry {:key "preparedStatements"}           "false"]
                                   [:entry {:key "Max open prepared statements"} "50"]
                                   ]]))])

(defn create-shapefile-data-store
  [{:keys [Workspace Store Description URI]}]
  ;;(println "create-shapefile-data-store" (str Workspace ":" Store))
  [(form-rest-uri (str "/workspaces/" Workspace "/datastores"))
   :method  "POST"
   :headers {"Accepts"       "application/xml",
             "Content-type"  "application/xml",
             "Authorization" *geoserver-auth-code*}
   :body    (with-out-str (prxml [:dataStore
                                  [:name Store]
                                  [:description Description]
                                  [:type "Shapefile"]
                                  [:enabled "true"]
                                  [:connectionParameters
                                   [:entry {:key "memory mapped buffer"}         "true"]
                                   [:entry {:key "create spatial index"}         "true"]
                                   [:entry {:key "charset"}                      "ISO-8859-1"]
                                   [:entry {:key "url"}                          URI]
                                   [:entry {:key "namespace"}                    (form-namespace Workspace)]
                                   ]]))])

(defn create-postgis-feature-type
  [{:keys [Workspace Store Layer Description NativeSRS DeclaredSRS]}]
  ;;(println "create-postgis-feature-type" (str Workspace ":" Store ":" Layer))
  [(form-rest-uri (str "/workspaces/" Workspace "/datastores/" Store "/featuretypes"))
   :method  "POST"
   :headers {"Accepts"       "application/xml",
             "Content-type"  "application/xml",
             "Authorization" *geoserver-auth-code*}
   :body    (with-out-str (prxml [:featureType
                                  [:name Layer]
                                  [:nativeName Layer]
                                  [:title Description]
                                  [:abstract Description]
                                  [:enabled "true"]
                                  [:maxFeatures "0"]
                                  [:numDecimals "0"]
                                  ]))])

(defn create-shapefile-feature-type
  [{:keys [Workspace Store Layer Description NativeSRS DeclaredSRS]}]
  ;;(println "create-shapefile-feature-type" (str Workspace ":" Store ":" Layer))
  [(form-rest-uri (str "/workspaces/" Workspace "/datastores/" Store "/featuretypes"))
   :method  "POST"
   :headers {"Accepts"       "application/xml",
             "Content-type"  "application/xml",
             "Authorization" *geoserver-auth-code*}
   :body    (with-out-str (prxml [:featureType
                                  [:name Layer]
                                  [:nativeName Store]
                                  [:title Description]
                                  [:abstract Description]
                                  [:enabled "true"]
                                  [:maxFeatures "0"]
                                  [:numDecimals "0"]
                                  ]))])

(defn create-coverage-store
  [{:keys [Workspace Store Description URI]}]
  ;;(println "create-coverage-store" (str Workspace ":" Store))
  [(form-rest-uri (str "/workspaces/" Workspace "/coveragestores"))
   :method  "POST"
   :headers {"Accepts"       "application/xml",
             "Content-type"  "application/xml",
             "Authorization" *geoserver-auth-code*}
   :body    (with-out-str (prxml [:coverageStore
                                  [:name Store]
                                  [:description Description]
                                  [:type "GeoTIFF"]
                                  [:enabled "true"]
                                  [:url URI]]))])

(defn extract-path
  [uri]
  (second (re-find #"^file:(.*)$" uri)))

(defn extract-filename
  [uri]
  (second (re-find #"^file:.*/([^/]*).tif$" uri)))

;;(defn create-coverage-via-put
;;  [{:keys [Workspace Store Layer Description URI NativeSRS DeclaredSRS]}]
;;  ;;(println "create-coverage-via-put" (str Workspace ":" Store ":" Layer))
;;  [(form-rest-uri (str "/workspaces/" Workspace "/coveragestores/" Store "/external.geotiff?configure=first&coverageName=" Layer))
;;   :method  "PUT"
;;   :headers {"Accepts"       "*/*",
;;             "Content-type"  "text/plain",
;;             "Authorization" *geoserver-auth-code*}
;;   :body    URI])

(defn create-coverage
  [{:keys [Workspace Store Layer Description URI NativeSRS DeclaredSRS]}]
  ;;(println "create-coverage" (str Workspace ":" Store ":" Layer))
  (let [gdal-info (with-sh-dir *geoserver-data-dir*
                    (:out (sh "gdalinfo" (extract-path URI))))]
    (println "gdal-info:\n" gdal-info)
    [(form-rest-uri (str "/workspaces/" Workspace "/coveragestores/" Store "/coverages"))
     :method  "POST"
     :headers {"Accepts"       "application/xml",
               "Content-type"  "application/xml",
               "Authorization" *geoserver-auth-code*}
     :body    (with-out-str (prxml [:coverage
                                    [:name Layer]
                                    [:title Description]
                                    [:description Description]
                                    [:abstract Description]
                                    [:enabled "true"]
                                    [:srs DeclaredSRS]
                                    [:projectionPolicy "FORCE_DECLARED"]
                                    [:keywords
                                     [:string "WCS"]
                                     [:string "GeoTIFF"]
                                     [:string (extract-filename URI)]]
                                    [:metadata
                                     [:entry {:key "dirName"} (extract-filename URI)]]
                                    [:nativeBoundingBox
                                     [:crs NativeSRS]]
                                    [:nativeFormat "GeoTIFF"]
                                    ]))]))

(defn translate-row
  [[current-workspace current-store current-uri xml-rows] row]
  (let [workspace    (or (:Workspace row) current-workspace)
        store        (or (:Store     row) current-store)
        uri          (or (:URI       row) current-uri)
        store-type   (if uri (get-store-type uri))
        complete-row (assoc row
                       :Workspace workspace
                       :Store     store
                       :URI       uri)]
    [workspace store uri
     (conj xml-rows
           (cond (:Layer row)
                 (condp = store-type
                   "GeoTIFF"
                   ((juxt create-coverage-store
                          create-coverage
                          create-layer)
                    complete-row)

                   "Shapefile"
                   ((juxt create-data-store
                          create-feature-type
                          create-layer)
                    complete-row)

                   "PostGIS Table"
                   (throw (Exception. "Creating a PostGIS Data Store from a Shapefile is not yet supported."))
               
                   "PostGIS Database"
                   ((juxt create-feature-type
                          create-layer)
                    complete-row)

                   :otherwise
                   (throw (Exception. (str "Unrecognized URI: " uri))))

                 (:Store row)
                 (condp = store-type
                     "GeoTIFF"          [(create-coverage-store complete-row)]
                     "Shapefile"        [(create-data-store complete-row)]
                     "PostGIS Table"    [(create-data-store complete-row)]
                     "PostGIS Database" [(create-data-store complete-row)])

                 (:Workspace row)
                 [(create-workspace-and-namespace complete-row)]

                 :otherwise
                 (throw (Exception. "Unrecognized row specification: " row))))]))

(defn rows->xml
  [rows]
  (last (reduce translate-row [nil nil nil []] rows)))

(defn rest-request
  [uri method user passwd xml-body]
  (http-agent uri
              :method method
              :headers {"Accepts"       "application/xml",
                        "Content-type"  "application/xml",
                        "Authorization" (str "Basic " (encode-str (str user ":" passwd)))}
              :body xml-body))

;;(defn -main
;;  [& args]
;;  (doseq [xml-body (rows->xml *raster-data*)]
    