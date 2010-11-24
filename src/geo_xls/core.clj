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
  (:gen-class)
  (:use [clojure.java.io              :only (reader)]
        [clojure.java.shell           :only (with-sh-dir sh)]
        [clojure.contrib.prxml        :only (prxml)]
        [clojure.contrib.base64       :only (encode-str)]
        [clojure.contrib.http.agent   :only (http-agent success? error? status message string)]
        [dk.ative.docjure.spreadsheet :only (load-workbook select-sheet select-columns)]))

(defn create-workspace-and-namespace
  [{:keys [namespace-prefix]} {:keys [Workspace]}]
  (println "create-workspace-and-namespace" Workspace)
  ["/namespaces"
   (with-out-str (prxml [:namespace
                         [:prefix Workspace]
                         [:uri (str namespace-prefix Workspace)]]))])

(defn extract-dbname
  [uri]
  (second (re-find #"^postgis:(.*)$" uri)))

(defn create-postgis-data-store
  [{:keys [namespace-prefix]} {:keys [Workspace Store Description URI]}]
  (println "create-postgis-data-store" (str Workspace ":" Store))
  [(str "/workspaces/" Workspace "/datastores")
   (with-out-str (prxml [:dataStore
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

                          [:entry {:key "namespace"}                    (str namespace-prefix Workspace)]
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
  [config-params row]
  (throw (Exception. "Creating a PostGIS Data Store from a Shapefile is not yet supported.")))

(defn create-shapefile-data-store
  [{:keys [namespace-prefix]} {:keys [Workspace Store Description URI]}]
  (println "create-shapefile-data-store" (str Workspace ":" Store))
  [(str "/workspaces/" Workspace "/datastores")
   (with-out-str (prxml [:dataStore
                         [:name Store]
                         [:description Description]
                         [:type "Shapefile"]
                         [:enabled "true"]
                         [:connectionParameters
                          [:entry {:key "memory mapped buffer"} "true"]
                          [:entry {:key "create spatial index"} "true"]
                          [:entry {:key "charset"}              "ISO-8859-1"]
                          [:entry {:key "url"}                  URI]
                          [:entry {:key "namespace"}            (str namespace-prefix Workspace)]
                          ]]))])

(defn create-postgis-feature-type
  [config-params {:keys [Workspace Store Layer Description]}]
  (println "create-postgis-feature-type" (str Workspace ":" Store ":" Layer))
  [(str "/workspaces/" Workspace "/datastores/" Store "/featuretypes")
   (with-out-str (prxml [:featureType
                         [:name Layer]
                         [:nativeName Layer]
                         [:title Description]
                         [:abstract Description]
                         [:enabled "true"]
                         [:maxFeatures "0"]
                         [:numDecimals "0"]
                         ]))])

(defn create-shapefile-feature-type
  [config-params {:keys [Workspace Store Layer Description]}]
  (println "create-shapefile-feature-type" (str Workspace ":" Store ":" Layer))
  [(str "/workspaces/" Workspace "/datastores/" Store "/featuretypes")
   (with-out-str (prxml [:featureType
                         [:name Layer]
                         [:nativeName Store]
                         [:title Description]
                         [:abstract Description]
                         [:enabled "true"]
                         [:maxFeatures "0"]
                         [:numDecimals "0"]
                         ]))])

(defn create-coverage-store
  [config-params {:keys [Workspace Store Description URI]}]
  (println "create-coverage-store" (str Workspace ":" Store))
  [(str "/workspaces/" Workspace "/coveragestores")
   (with-out-str (prxml [:coverageStore
                         [:name Store]
                         [:description Description]
                         [:type "GeoTIFF"]
                         [:enabled "true"]
                         [:workspace
                          [:name Workspace]]
                         [:url URI]]))])

(defn extract-filename
  [uri]
  (second (re-find #"^file:.*/([^/]*).tif$" uri)))

(defn extract-path
  [uri]
  (second (re-find #"^file:(.*)$" uri)))

(defn run-gdal-info
  [geoserver-data-dir uri]
  (with-sh-dir geoserver-data-dir
    (:out (sh "gdalinfo" (extract-path uri)))))

(defn dms->dd
  [dms]
  (let [[d m s dir] (map read-string (rest (re-find #"^([ \d]+)d([ \d]+)'([ \.0123456789]+)\"(\w)$" dms)))
        unsigned-dd (+ d (/ m 60.0) (/ s 3600.0))]
    (if (#{'S 'W} dir)
      (- unsigned-dd)
      unsigned-dd)))

(defn radians->degrees
  [rads]
  (/ (* rads 180.0) Math/PI))

(defn extract-georeferences
  [geoserver-data-dir uri]
  (let [gdal-info          (run-gdal-info geoserver-data-dir uri)
        cols-rows-regex    #"(?s)Size is (\d+), (\d+)"
        pixel-size-regex   #"(?s)Pixel Size = \(([\-\.0123456789]+),([\-\.0123456789]+)\)"
        origin-regex       #"(?s)Origin = \(([\-\.0123456789]+),([\-\.0123456789]+)\)"

        color-interp-regex #"(?s)ColorInterp=(\w+)"
        native-crs-regex   #"(?s)Coordinate System is:\s*\n(.+)Origin"

        upper-left-regex   #"(?s)Upper Left\s+\(\s*([\-\.0123456789]+),\s+([\-\.0123456789]+)\)\s+\(\s*([^,]+),\s*([^\)]+)\)"
        lower-left-regex   #"(?s)Lower Left\s+\(\s*([\-\.0123456789]+),\s+([\-\.0123456789]+)\)\s+\(\s*([^,]+),\s*([^\)]+)\)"
        upper-right-regex  #"(?s)Upper Right\s+\(\s*([\-\.0123456789]+),\s+([\-\.0123456789]+)\)\s+\(\s*([^,]+),\s*([^\)]+)\)"
        lower-right-regex  #"(?s)Lower Right\s+\(\s*([\-\.0123456789]+),\s+([\-\.0123456789]+)\)\s+\(\s*([^,]+),\s*([^\)]+)\)"

        [cols rows]                (rest (re-find cols-rows-regex  gdal-info))
        [pixel-width pixel-height] (rest (re-find pixel-size-regex gdal-info))
        [x-origin y-origin]        (rest (re-find origin-regex     gdal-info))

        [ul-native-x ul-native-y ul-latlon-x ul-latlon-y] (rest (re-find upper-left-regex gdal-info))
        [ll-native-x ll-native-y ll-latlon-x ll-latlon-y] (rest (re-find lower-left-regex gdal-info))
        [ur-native-x ur-native-y ur-latlon-x ur-latlon-y] (rest (re-find upper-right-regex gdal-info))
        [lr-native-x lr-native-y lr-latlon-x lr-latlon-y] (rest (re-find lower-right-regex gdal-info))

        [ul-native-x ul-native-y] (map read-string [ul-native-x ul-native-y])
        [ll-native-x ll-native-y] (map read-string [ll-native-x ll-native-y])
        [ur-native-x ur-native-y] (map read-string [ur-native-x ur-native-y])
        [lr-native-x lr-native-y] (map read-string [lr-native-x lr-native-y])]

    {:cols-rows    (str cols " " rows)
     :pixel-width  pixel-width
     :pixel-height pixel-height
     :x-origin     x-origin
     :y-origin     y-origin
     :color-interp (second (re-find color-interp-regex gdal-info))
     :nativeCRS    (second (re-find native-crs-regex gdal-info))
     :native-min-x (str (min ul-native-x ll-native-x))
     :native-max-x (str (max ur-native-x lr-native-x))
     :native-min-y (str (min ll-native-y lr-native-y))
     :native-max-y (str (max ul-native-y ur-native-y))
     :latlon-min-x (str (apply min (map dms->dd [ul-latlon-x ll-latlon-x])))
     :latlon-max-x (str (apply max (map dms->dd [ur-latlon-x lr-latlon-x])))
     :latlon-min-y (str (apply min (map dms->dd [ll-latlon-y lr-latlon-y])))
     :latlon-max-y (str (apply max (map dms->dd [ul-latlon-y ur-latlon-y])))
     :shear-x      (str (radians->degrees (Math/asin (/ (- ll-native-x ul-native-x) (- ul-native-y ll-native-y)))))
     :shear-y      (str (radians->degrees (Math/asin (/ (- ur-native-y ul-native-y) (- ur-native-x ul-native-x)))))}))

(defn create-coverage
  [{:keys [geoserver-data-dir]} {:keys [Workspace Store Layer Description URI NativeSRS DeclaredSRS]}]
  (println "create-coverage" (str Workspace ":" Store ":" Layer))
  (let [gdal-info (extract-georeferences geoserver-data-dir URI)]
    [(str "/workspaces/" Workspace "/coveragestores/" Store "/coverages")
     (with-out-str (prxml [:coverage
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
                            [:minx (:native-min-x gdal-info)]
                            [:maxx (:native-max-x gdal-info)]
                            [:miny (:native-min-y gdal-info)]
                            [:maxy (:native-max-y gdal-info)]
                            (if (and NativeSRS (not= NativeSRS "UNKNOWN"))
                              [:crs NativeSRS])
                            ]
                           [:latLonBoundingBox
                            [:minx (:latlon-min-x gdal-info)]
                            [:maxx (:latlon-max-x gdal-info)]
                            [:miny (:latlon-min-y gdal-info)]
                            [:maxy (:latlon-max-y gdal-info)]
                            [:crs "EPSG:4326"]]
                           [:projectionPolicy "REPROJECT_TO_DECLARED"]
                           [:metadata
                            [:entry {:key "cachingEnabled"} "false"]
                            [:entry {:key "dirName"} (str Store "_" (extract-filename URI))]]
                           [:nativeFormat "GeoTIFF"]
                           [:grid {:dimension "2"}
                            [:range
                             [:low "0 0"]
                             [:high (:cols-rows gdal-info)]]
                            [:transform
                             [:scaleX (:pixel-width  gdal-info)]
                             [:scaleY (:pixel-height gdal-info)]
                             [:shearX (:shear-x gdal-info)]
                             [:shearY (:shear-y gdal-info)]
                             [:translateX (:x-origin gdal-info)]
                             [:translateY (:y-origin gdal-info)]]
                            [:crs DeclaredSRS]]
                           [:supportedFormats
                            [:string "GIF"]
                            [:string "PNG"]
                            [:string "JPEG"]
                            [:string "TIFF"]
                            [:string "GEOTIFF"]]
                           [:interpolationMethods
                            [:string "bilinear"]
                            [:string "bicubic"]]
                           [:dimensions
                            [:coverageDimension
                             [:name (.toUpperCase (str (:color-interp gdal-info) "_INDEX"))]
                             [:description "GridSampleDimension[-Infinity,Infinity]"]]]
                           [:requestSRS
                            [:string "EPSG:4326"]
                            (if (not= DeclaredSRS "EPSG:4326")
                              [:string DeclaredSRS])]
                           [:responseSRS
                            [:string "EPSG:4326"]
                            (if (not= DeclaredSRS "EPSG:4326")
                              [:string DeclaredSRS])]
                           ]))]))

(defn get-store-type
  [uri]
  (condp re-matches uri
    #"^file:.*\.tif$"    "GeoTIFF"
    #"^file:.*\.shp$"    "Shapefile"
    #"^postgis:.*\.shp$" "PostGIS-converted Shapefile"
    #"^postgis:.*$"      "PostGIS Database"))

(defn translate-row
  [config-params [current-workspace current-store current-uri xml-rows] row]
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
                    config-params complete-row)

                   "Shapefile"
                   ((juxt create-shapefile-data-store
                          create-shapefile-feature-type)
                    config-params complete-row)

                   "PostGIS-converted Shapefile"
                   ((juxt create-postgis-data-store-from-shapefile
                          create-postgis-feature-type)
                    config-params complete-row)

                   "PostGIS Database"
                   [(create-postgis-feature-type config-params complete-row)]

                   :otherwise
                   (throw (Exception. (str "Unrecognized URI: " uri))))

                 (:Store row)
                 (condp = store-type
                     "GeoTIFF"                     [(create-coverage-store config-params complete-row)]
                     "Shapefile"                   [(create-shapefile-data-store config-params complete-row)]
                     "PostGIS-converted Shapefile" [(create-postgis-data-store-from-shapefile config-params complete-row)]
                     "PostGIS Database"            [(create-postgis-data-store config-params complete-row)])

                 (:Workspace row)
                 [(create-workspace-and-namespace config-params complete-row)]

                 :otherwise
                 (throw (Exception. "Unrecognized row specification: " row))))]))

(defn rows->xml
  [config-params spreadsheet-rows]
  (apply concat
         (last
          (reduce (partial translate-row config-params)
                  [nil nil nil []]
                  spreadsheet-rows))))

(defn make-rest-request
  [{:keys [geoserver-rest-uri geoserver-auth-code]}
   [uri-suffix xml-body]]
  (let [agnt (http-agent (str geoserver-rest-uri uri-suffix)
                         :method  "POST"
                         :headers {"Accepts"       "application/xml",
                                   "Content-type"  "application/xml",
                                   "Authorization" geoserver-auth-code}
                         :body    xml-body)]
    (await agnt)
    (if (success? agnt)
      (do (print "*") (flush))
      (do (print "!") (flush)))
    agnt))

(defn select-addition-rows
  [spreadsheet-rows]
  (filter (fn [{workspace :Workspace}] (and workspace (re-matches #"^\+.*" workspace))) spreadsheet-rows))

(defn select-deletion-rows
  [spreadsheet-rows]
  (filter (fn [{workspace :Workspace}] (and workspace (re-matches #"^\-.*" workspace))) spreadsheet-rows))

(defn remove-comment-rows
  [spreadsheet-rows]
  (remove (fn [{workspace :Workspace}] (and workspace (re-matches #"^#.*" workspace))) spreadsheet-rows))

(defn load-column-data
  [spreadsheet-filename sheet-name column-spec]
  (->> (load-workbook spreadsheet-filename)
       (select-sheet sheet-name)
       (select-columns column-spec)
       next))

(defn update-geoserver
  [{:keys [spreadsheet-filename spreadsheet-sheetname column-spec] :as config-params}]
  (let [http-agents   (->> (load-column-data spreadsheet-filename spreadsheet-sheetname column-spec)
                           remove-comment-rows
                           (rows->xml config-params)
                           (map (partial make-rest-request config-params)))
        failed-agents (filter error? http-agents)]
    (println "\nFinished updating Geoserver.\nSuccessful requests:"
             (- (count http-agents) (count failed-agents))
             "\nFailed requests:"
             (count failed-agents)
             "\n\nErrors:")
    (doseq [agent-info (map (juxt status message string) failed-agents)]
      (println agent-info))))

(defn read-config-params
  [config-file-path]
  (with-open [config-file (java.io.PushbackReader. (reader config-file-path))]
    (read config-file)))

(defn -main
  [config-file-path]
  (let [config-params (read-config-params config-file-path)]
    (update-geoserver
     (assoc config-params
       :geoserver-auth-code
       (str "Basic " (encode-str (str (:geoserver-username config-params)
                                      ":"
                                      (:geoserver-password config-params))))))))
