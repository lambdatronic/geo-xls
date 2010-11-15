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

;; ===== Begin global parameters =====

(def *spreadsheet-filename*  "/home/gjohnson/code/clojure/projects/geo-xls/resources/Geoserver_REST_database_combined.xls")
(def *spreadsheet-sheetname* "Sheet1")

(def *column-spec*
     {:A :Workspace
      :B :Store
      :D :Layer
      :E :Description
      :F :URI
      :G :DefaultStyle
      :K :NativeSRS
      :L :DeclaredSRS})

(def *geoserver-rest-uri*     "http://ecoinformatics.uvm.edu/geoserver/rest")
(def *geoserver-username*     "admin")
(def *geoserver-password*     "rnbh304")
(def *geoserver-auth-code*    (str "Basic " (encode-str (str *geoserver-username* ":" *geoserver-password*))))
(def *geoserver-data-dir*     "/raid/geodata/")
(def *aries-namespace-prefix* "http://www.integratedmodelling.org/geo/ns/")

;; ===== End global parameters =====

(defn load-column-data
  [spreadsheet-filename sheet-name column-spec]
  (->> (load-workbook spreadsheet-filename)
       (select-sheet sheet-name)
       (select-columns column-spec)
       next))

(defn get-store-type
  [uri]
  (condp re-matches uri
    #"^file:.*\.tif$"    "GeoTIFF"
    #"^file:.*\.shp$"    "Shapefile"
    #"^postgis:.*\.shp$" "PostGIS-converted Shapefile"
    #"^postgis:.*$"      "PostGIS Database"))

(defn form-rest-uri
  [uri-suffix]
  (str *geoserver-rest-uri* uri-suffix))

(defn form-namespace
  [Workspace]
  (str *aries-namespace-prefix* Workspace))

(defn create-workspace-and-namespace
  [{:keys [Workspace]}]
  (println "create-workspace-and-namespace" Workspace)
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
  (println "create-postgis-data-store" (str Workspace ":" Store))
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

(defn create-postgis-data-store-from-shapefile
  [row]
  (throw (Exception. "Creating a PostGIS Data Store from a Shapefile is not yet supported.")))

(defn create-shapefile-data-store
  [{:keys [Workspace Store Description URI]}]
  (println "create-shapefile-data-store" (str Workspace ":" Store))
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
  (println "create-postgis-feature-type" (str Workspace ":" Store ":" Layer))
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
  (println "create-shapefile-feature-type" (str Workspace ":" Store ":" Layer))
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
  (println "create-coverage-store" (str Workspace ":" Store))
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

(defn run-gdal-info
  [uri]
  (with-sh-dir *geoserver-data-dir*
    (:out (sh "gdalinfo" (extract-path uri)))))

(defn dms->dd
  [dms]
  (let [[d m s dir] (map read-string (rest (re-find #"^([ \d]+)d([ \d]+)'([ \.0123456789]+)\"(\w)$" dms)))
        unsigned-dd (float (+ d (/ m 60) (/ s 3600)))]
    (if (#{'S 'W} dir)
      (- unsigned-dd)
      unsigned-dd)))

(defn extract-georeferences
  [uri]
  (let [gdal-info (run-gdal-info uri)
        native-crs-regex #"(?s)Coordinate System is:\s*\n(.+)Origin"
        upper-left-regex #"(?s)Upper Left\s+\(\s*([\-\.0123456789]+),\s+([\-\.0123456789]+)\)\s+\(\s*([^,]+),\s*([^\)]+)\)"
        lower-right-regex #"(?s)Lower Right\s+\(\s*([\-\.0123456789]+),\s+([\-\.0123456789]+)\)\s+\(\s*([^,]+),\s*([^\)]+)\)"
        [nminx nmaxy llminx llmaxy] (rest (re-find upper-left-regex gdal-info))
        [nmaxx nminy llmaxx llminy] (rest (re-find lower-right-regex gdal-info))]
    {:nativeCRS (second (re-find native-crs-regex gdal-info))
     :nminx     nminx
     :nmaxx     nmaxx
     :nminy     nminy
     :nmaxy     nmaxy
     :llminx    (str (dms->dd llminx))
     :llmaxx    (str (dms->dd llmaxx))
     :llminy    (str (dms->dd llminy))
     :llmaxy    (str (dms->dd llmaxy))}))

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
  (println "create-coverage" (str Workspace ":" Store ":" Layer))
  (let [gdal-info (extract-georeferences URI)]
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
                                    [:keywords
                                     [:string "WCS"]
                                     [:string "GeoTIFF"]
                                     [:string (extract-filename URI)]]
                                    [:nativeCRS (:nativeCRS gdal-info)]
                                    [:srs DeclaredSRS]
                                    [:nativeBoundingBox
                                     [:minx (:nminx gdal-info)]
                                     [:maxx (:nmaxx gdal-info)]
                                     [:miny (:nminy gdal-info)]
                                     [:maxy (:nmaxy gdal-info)]
                                     [:crs NativeSRS]]
                                    [:latLonBoundingBox
                                     [:minx (:llminx gdal-info)]
                                     [:maxx (:llmaxx gdal-info)]
                                     [:miny (:llminy gdal-info)]
                                     [:maxy (:llmaxy gdal-info)]
                                     [:crs "EPSG:4326"]]
;;                                    [:projectionPolicy (if (not= NativeSRS DeclaredSRS) "FORCE_DECLARED" "KEEP_NATIVE")]
;;                                    [:projectionPolicy "REPROJECT_NATIVE_TO_DECLARED"]
                                    [:metadata
                                     [:entry {:key "dirName"} (str Store "_" (extract-filename URI))]]
                                    [:nativeFormat "GeoTIFF"]
                                    [:requestSRS
                                     [:string "EPSG:4326"]
                                     (if (not= DeclaredSRS "EPSG:4326")
                                       [:string DeclaredSRS])]
                                    [:responseSRS
                                     [:string "EPSG:4326"]
                                     (if (not= DeclaredSRS "EPSG:4326")
                                       [:string DeclaredSRS])]
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
                          create-coverage)
                    complete-row)

                   "Shapefile"
                   ((juxt create-shapefile-data-store
                          create-shapefile-feature-type)
                    complete-row)

                   "PostGIS-converted Shapefile"
                   (throw (Exception. "Creating a PostGIS Data Store from a Shapefile is not yet supported."))

                   "PostGIS Database"
                   [(create-postgis-feature-type complete-row)]

                   :otherwise
                   (throw (Exception. (str "Unrecognized URI: " uri))))

                 (:Store row)
                 (condp = store-type
                     "GeoTIFF"                     [(create-coverage-store complete-row)]
                     "Shapefile"                   [(create-shapefile-data-store complete-row)]
                     "PostGIS-converted Shapefile" [(create-postgis-data-store-from-shapefile complete-row)]
                     "PostGIS Database"            [(create-postgis-data-store complete-row)])

                 (:Workspace row)
                 [(create-workspace-and-namespace complete-row)]

                 :otherwise
                 (throw (Exception. "Unrecognized row specification: " row))))]))

(defn rows->xml
  [spreadsheet-rows]
  (last (reduce translate-row [nil nil nil []] spreadsheet-rows)))

(defn remove-comment-rows
  [spreadsheet-rows]
  (remove (fn [{workspace :Workspace}] (re-matches "^#.*" workspace)) spreadsheet-rows))

(defn make-rest-request
  [rest-spec]
  (let [agnt (apply http-agent rest-spec)]
    (await agnt)
    agnt))

(defn main
  [& args]
  (let [row-data       (remove-comment-rows
                        (load-column-data *spreadsheet-filename* *spreadsheet-sheetname* *column-spec*))
        http-agents    (for [rest-specs (rows->xml row-data), rest-spec rest-specs]
                         (make-rest-request rest-spec))
        failed-agents  (filter error? http-agents)]
    (println "Finished repopulating Geoserver.\nSuccessful requests:"
             (- (count http-agents) (count failed-agents))
             "\nFailed requests:" (count failed-agents)
             "\n\nErrors:")
    (doseq [agent-info (map (juxt status message string) (failed-agents))]
      (println agent-info))))
