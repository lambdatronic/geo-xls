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
  (:use [clojure.set                  :only (map-invert)]
        [clojure.java.io              :only (reader)]
        [clojure.java.shell           :only (with-sh-dir sh)]
        [clojure.contrib.prxml        :only (prxml)]
        [clojure.contrib.base64       :only (encode-str)]
        [clojure.contrib.http.agent   :only (http-agent error? status message string)]
        [clojure.contrib.command-line :only (with-command-line)]
        [dk.ative.docjure.spreadsheet :only (load-workbook select-sheet select-columns)]))

(defn make-rest-request
  [{:keys [geoserver-rest-uri geoserver-rest-http-headers]}
   [http-method uri-suffix http-body]]
  (let [agnt (http-agent (str geoserver-rest-uri uri-suffix)
                         :method  http-method
                         :headers (geoserver-rest-http-headers http-method)
                         :body    http-body)]
    (await agnt)
    agnt))

(defn remove-prefix
  "If string begins with a + or -, returns the string without this
   initial symbol."
  [string]
  (or (second (re-matches #"^[\+\-](.*)" string))
      string))

(defn create-workspace-and-namespace
  [{:keys [namespace-prefix]} {:keys [Workspace]}]
  (println "create-workspace-and-namespace" Workspace)
  ["POST"
   "/namespaces"
   (with-out-str (prxml [:namespace
                         [:prefix Workspace]
                         [:uri (str namespace-prefix Workspace)]]))])

(defn delete-workspace-and-namespace
  [config-params {:keys [Workspace]}]
  (println "delete-workspace-and-namespace" Workspace)
  ["DELETE"
   (str "/workspaces/" Workspace)
   nil])

(defn extract-dbname
  [uri]
  (second (re-find #"^postgis:(.*)$" uri)))

(defn create-postgis-data-store
  [{:keys [namespace-prefix]} {:keys [Workspace Store Description URI]}]
  (println "create-postgis-data-store" (str Workspace ":" Store))
  ["POST"
   (str "/workspaces/" Workspace "/datastores")
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

(defn delete-postgis-data-store
  [config-params {:keys [Workspace Store]}]
  (println "delete-postgis-data-store" (str Workspace ":" Store))
  ["DELETE"
   (str "/workspaces/" Workspace "/datastores/" Store)
   nil])

(defn add-shapefile-to-postgis-db
  [config-params {:keys [Workspace Store Layer URI]}]
  (throw (Exception. (str "Adding a Shapefile to a PostGIS Database is not yet supported: "
                          Workspace ":" Store ":" Layer " (" URI ")"))))

(defn remove-shapefile-from-postgis-db
  [config-params {:keys [Workspace Store Layer URI]}]
  (throw (Exception. (str "Removing a Shapefile from a PostGIS Database is not yet supported: "
                          Workspace ":" Store ":" Layer " (" URI ")"))))

(defn create-postgis-feature-type
  [config-params {:keys [Workspace Store Layer Description]}]
  (println "create-postgis-feature-type" (str Workspace ":" Store ":" Layer))
  ["POST"
   (str "/workspaces/" Workspace "/datastores/" Store "/featuretypes")
   (with-out-str (prxml [:featureType
                         [:name Layer]
                         [:nativeName Layer]
                         [:title Description]
                         [:abstract Description]
                         [:enabled "true"]
                         [:maxFeatures "0"]
                         [:numDecimals "0"]
                         ]))])

(defn delete-postgis-feature-type
  [config-params {:keys [Workspace Store Layer]}]
  (println "delete-postgis-feature-type" (str Workspace ":" Store ":" Layer))
  ["DELETE"
   (str "/workspaces/" Workspace "/datastores/" Store "/featuretypes/" Layer)
   nil])

(defn create-shapefile-data-store
  [{:keys [namespace-prefix]} {:keys [Workspace Store Description URI]}]
  (println "create-shapefile-data-store" (str Workspace ":" Store))
  ["POST"
   (str "/workspaces/" Workspace "/datastores")
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

(defn delete-shapefile-data-store
  [config-params {:keys [Workspace Store]}]
  (println "delete-shapefile-data-store" (str Workspace ":" Store))
  ["DELETE"
   (str "/workspaces/" Workspace "/datastores/" Store)
   nil])

(defn create-shapefile-feature-type
  [config-params {:keys [Workspace Store Layer Description]}]
  (println "create-shapefile-feature-type" (str Workspace ":" Store ":" Layer))
  ["POST"
   (str "/workspaces/" Workspace "/datastores/" Store "/featuretypes")
   (with-out-str (prxml [:featureType
                         [:name Layer]
                         [:nativeName Store]
                         [:title Description]
                         [:abstract Description]
                         [:enabled "true"]
                         [:maxFeatures "0"]
                         [:numDecimals "0"]
                         ]))])

(defn create-shapefile-feature-type-via-put
  [config-params {:keys [Workspace Store URI]}]
  (println "create-shapefile-feature-type-via-put" (str Workspace ":" Store))
  ["PUT"
   (str "/workspaces/" Workspace "/datastores/" Store "/external.shp?configure=first")
   URI])

(defn delete-shapefile-feature-type
  [config-params {:keys [Workspace Store Layer]}]
  (println "delete-shapefile-feature-type" (str Workspace ":" Store ":" Layer))
  ["DELETE"
   (str "/workspaces/" Workspace "/datastores/" Store "/featuretypes/" Layer)
   nil])

(defn create-coverage-store
  [config-params {:keys [Workspace Store Description URI]}]
  (println "create-coverage-store" (str Workspace ":" Store))
  ["POST"
   (str "/workspaces/" Workspace "/coveragestores")
   (with-out-str (prxml [:coverageStore
                         [:name Store]
                         [:description Description]
                         [:type "GeoTIFF"]
                         [:enabled "true"]
                         [:workspace
                          [:name Workspace]]
                         [:url URI]]))])

(defn delete-coverage-store
  [config-params {:keys [Workspace Store]}]
  (println "delete-coverage-store" (str Workspace ":" Store))
  ["DELETE"
   (str "/workspaces/" Workspace "/coveragestores/" Store)
   nil])

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
    ["POST"
     (str "/workspaces/" Workspace "/coveragestores/" Store "/coverages")
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

(defn delete-coverage
  [config-params {:keys [Workspace Store Layer]}]
  (println "delete-coverage" (str Workspace ":" Store ":" Layer))
  ["DELETE"
   (str "/workspaces/" Workspace "/coveragestores/" Store "/coverages/" Layer)
   nil])

(defn delete-layer
  [config-params {:keys [Workspace Store Layer]}]
  (println "delete-layer" (str Workspace ":" Store ":" Layer))
  ["DELETE"
   (str "/layers/" Layer)
   nil])

(defn get-store-type
  "Returns a string describing the class of data or coverage store
   implied by the structure of the passed-in URI."
  [URI]
  (condp re-matches URI
    #"^file:.*\.tif$"    "GeoTIFF"
    #"^file:.*\.shp$"    "Shapefile"
    #"^postgis:.*\.shp$" "PostGIS-converted Shapefile"
    #"^postgis:.*$"      "PostGIS Database"
    :otherwise           (throw (Exception. (str "Unrecognized URI: " URI)))))

(defn translate-row
  "Returns a vector of one or more REST request specifications as
   triplets of [http-method uri-suffix http-body] depending on the
   contents of the passed-in row."
  [config-params {:keys [Workspace Store Layer URI Delete?] :as row}]
  (if URI
    (let [store-type (get-store-type URI)]
      (cond Layer
            (condp = store-type
              "GeoTIFF"
              (if Delete?
                ((juxt delete-layer delete-coverage delete-coverage-store) config-params row)
                ((juxt create-coverage-store create-coverage) config-params row))

              "Shapefile"
              (if Delete?
                ((juxt delete-layer delete-shapefile-feature-type delete-shapefile-data-store) config-params row)
                ((juxt create-shapefile-data-store create-shapefile-feature-type) config-params row))

              "PostGIS-converted Shapefile"
              (if Delete?
                ((juxt delete-layer delete-postgis-feature-type remove-shapefile-from-postgis-db) config-params row)
                ((juxt add-shapefile-to-postgis-db create-postgis-feature-type) config-params row))

              "PostGIS Database"
              (if Delete?
                ((juxt delete-layer delete-postgis-feature-type) config-params row)
                [(create-postgis-feature-type config-params row)]))

            Store
            (if (= store-type "PostGIS Database")
              (if Delete?
                [(delete-postgis-data-store config-params row)]
                [(create-postgis-data-store config-params row)])
              (throw (Exception. (str "Cannot declare file-based store without layer on same row: " Workspace ":" Store " (" URI ")"))))

            :otherwise (throw (Exception. (str "A row with a defined URI must also declare either a new Store or Layer: "
                                               Workspace " (" URI ")")))))

    (if (and Workspace
             (nil? Store)
             (nil? Layer))
      (if Delete?
        [(delete-workspace-and-namespace config-params row)]
        [(create-workspace-and-namespace config-params row)])
      (throw (Exception. "Rows without URIs must declare new workspaces: " row)))))

(defn rows->xml
  "Generates a sequence of REST request specifications as triplets of
   [http-method uri-suffix http-body].  Each spreadsheet-row may
   contribute one or more of these to the final sequence."
  [config-params spreadsheet-rows]
  (apply concat
         (map (partial translate-row config-params) spreadsheet-rows)))

(defn extract-plus-rows
  "Selects all the spreadsheet-rows, whose :Workspace field begins
   with a + and returns them with the + prefix removed."
  [spreadsheet-rows]
  (map #(update-in % [:Workspace] remove-prefix)
       (filter #(.startsWith (:Workspace %) "+") spreadsheet-rows)))

(defn extract-minus-rows
  "Selects all the spreadsheet-rows, whose :Workspace field begins
   with a -, removes the - prefix from this value, and adds the
   key-value pair {:Delete? true} to each row's map."
  [spreadsheet-rows]
  (map (comp #(assoc % :Delete? true)
             #(update-in % [:Workspace] remove-prefix))
       (filter #(.startsWith (:Workspace %) "-") spreadsheet-rows)))

(defn select-active-rows
  "Selects all the spreadsheet-rows, whose :Workspace field begins
   with either a + or -.  Any rows found will have the + or - prefix
   removed from their :Workspace values.  Rows with a - prefix will
   also have the key-value pair {:Delete? true} added to their row
   maps.  Rows with a - prefix will be sorted before those with the +
   prefix in the returned sequence.  If no + or - rows exist, returns
   all the spreadsheet-rows."
  [spreadsheet-rows]
  (let [plus-rows  (extract-plus-rows  spreadsheet-rows)
        minus-rows (extract-minus-rows spreadsheet-rows)]
    (if (and (empty? plus-rows) (empty? minus-rows))
      spreadsheet-rows
      (concat minus-rows plus-rows))))

(defn complete-row
  "Given two maps (previous-row and current-row), returns the
   current-row map with any empty :Workspace, :Store, and :URI fields
   filled in with the corresponding values from the previous-row map.
   Any :Workspace fields beginning with a + or - will not propagate
   this prefix symbol to its following row.  :Store and :URI field
   values will not propagate to a row which declares a
   new :Workspace."
  [{prev-workspace-raw :Workspace prev-store :Store prev-uri :URI}
   {curr-workspace-raw :Workspace curr-store :Store curr-uri :URI :as curr-row}]
  (let [prev-workspace      (remove-prefix prev-workspace-raw)
        curr-new-workspace? (if curr-workspace-raw (not (.isEmpty (remove-prefix curr-workspace-raw))))]
    (assoc curr-row
      :Workspace (if curr-workspace-raw
                   (if curr-new-workspace?
                     curr-workspace-raw
                     (str curr-workspace-raw prev-workspace))
                   prev-workspace)
      :Store     (or curr-store (if-not curr-new-workspace? prev-store))
      :URI       (or curr-uri   (if-not curr-new-workspace? prev-uri)))))

(defn complete-rows
  "Fills in empty Workspace, Store, and URI fields in each map in
   spreadsheet-rows by propagating forward the values from earlier
   maps to those which come immediately after them.  Never overwrites
   a field which already has a value."
  [spreadsheet-rows]
  (rest (reductions complete-row {:Workspace ""} spreadsheet-rows)))

(defn remove-comment-rows
  "Removes any maps from the spreadsheet-rows vector, whose :Workspace
   field begins with a #."
  [spreadsheet-rows]
  (remove (fn [{workspace :Workspace}] (and workspace (.startsWith workspace "#")))
          spreadsheet-rows))

(defn load-column-data
  "Reads the columns in column-spec from sheet-name in
   spreadsheet-filename.  Returns a vector of maps (one per row in the
   spreadsheet), whose fields correspond to the columns read from the
   spreadsheet.  Discards the first row in the sheet, assuming it only
   contains the column headers."
  [spreadsheet-filename sheet-name column-spec]
  (->> (load-workbook spreadsheet-filename)
       (select-sheet sheet-name)
       (select-columns (map-invert column-spec))
       next))

(defn update-geoserver
  "Loads the row data in from the spreadsheet according to
   the :spreadsheet-filename, :spreadsheet-sheetname, and :column-spec
   config-params.  Rows beginning with a # are ignored.  If any rows
   begin with a + or -, then only those rows will be processed
   further.  Otherwise, all spreadsheet rows will be processed by the
   algorithm.  For each row to be processed, a triplet of [http-method
   uri-suffix http-body] is created to describe the REST request that
   will be sent to our geoserver for that row.  Each such pair is then
   sent off as a REST request, and the number of successful and failed
   requests is printed to STDOUT along with the error messages for any
   failed requests."
  [{:keys [spreadsheet-filename spreadsheet-sheetname column-spec] :as config-params}]
  (let [http-agents   (->> (load-column-data spreadsheet-filename spreadsheet-sheetname column-spec)
                           remove-comment-rows
                           complete-rows
                           select-active-rows
                           (rows->xml config-params)
                           (map (partial make-rest-request config-params)))
        failed-agents (filter error? http-agents)]
    (println "\nFinished updating Geoserver.\nSuccessful requests:"
             (- (count http-agents) (count failed-agents))
             "\nFailed requests:"
             (count failed-agents)
             "\n\nErrors:")
    (if (empty? failed-agents)
      (println "None")
      (doseq [agent-info (map (juxt status message string) failed-agents)]
        (println agent-info)))))

(defn read-config-params
  "Opens config-file-path as a java.io.PushbackReader and calls the
   Clojure Reader on it once in order to load the first object in the
   file in as a Clojure data structure.  If the object is a Clojure
   map whose keys are keywords, it will be returned.  If
   config-file-path is nil, returns {}."
  [config-file-path]
  (if config-file-path
    (let [file-params (with-open [config-file (java.io.PushbackReader. (reader config-file-path))]
                        (read config-file))]
      (if (and (map? file-params)
               (every? keyword? (keys file-params)))
        file-params
        (throw (Exception. (str "The config-file must contain a clojure map whose keys are keywords: " config-file-path)))))
    {}))

(defn -main
  "AOT-compiled application entry point.
   Call it with the name of a Clojure file containing the
   config-params map.  The params will be read into a hash-map and
   passed on to the update-geoserver function.  So that we only have
   to calculate it once, the geoserver-auth-code is generated here
   from the :geoserver-username and :geoserver-password fields in the
   passed-in map and added to the in-memory hash-map under
   the :geoserver-rest-http-headers entry."
  [& args]
  (if (empty? args)
    (-main "-h")
    (with-command-line args
      (str "geo-xls: Update a running Geoserver instance from an XLS spreadsheet.\n"
           "Copyright 2010 Gary W. Johnson (gwjohnso@uvm.edu)\n")
      [[config-file           i "Path to a clojure file containing a map of configuration parameters."]
       [spreadsheet-filename  f "Path to the XLS spreadsheet."]
       [spreadsheet-sheetname s "Sheet name to use from the spreadsheet."]
       [column-spec           c "Map of required spreadsheet fields to their column letters."]
       [namespace-prefix      n "URI prefix for constructing namespaces from workspace names."]
       [geoserver-rest-uri    g "URI of your Geoserver's REST extensions."]
       [geoserver-username    u "Geoserver admin username."]
       [geoserver-password    p "Geoserver admin password."]
       [geoserver-data-dir    d "Path to your Geoserver's data_dir."]]
      (let [config-file-params  (read-config-params config-file)
            command-line-params (into {} (remove (comp nil? val)
                                                 {:spreadsheet-filename  spreadsheet-filename
                                                  :spreadsheet-sheetname spreadsheet-sheetname
                                                  :column-spec           column-spec
                                                  :namespace-prefix      namespace-prefix
                                                  :geoserver-rest-uri    geoserver-rest-uri
                                                  :geoserver-username    geoserver-username
                                                  :geoserver-password    geoserver-password
                                                  :geoserver-data-dir    geoserver-data-dir}))
            config-params       (merge config-file-params command-line-params)
            geoserver-auth-code (str "Basic " (encode-str (str (:geoserver-username config-params)
                                                               ":"
                                                               (:geoserver-password config-params))))]
        (update-geoserver
         (assoc config-params
           :geoserver-rest-http-headers {"POST"   {"Accepts"       "application/xml"
                                                   "Content-type"  "application/xml"
                                                   "Authorization" geoserver-auth-code}
                                         "PUT"    {"Accepts"       "*/*"
                                                   "Content-type"  "text/plain"
                                                   "Authorization" geoserver-auth-code}
                                         "DELETE" {"Accepts"       "*/*"
                                                   "Content-type"  "*/*"
                                                   "Authorization" geoserver-auth-code}
                                         })))))
  ;; Exit cleanly.
  (shutdown-agents)
  (flush)
  (System/exit 0))
