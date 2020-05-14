(ns core.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.pprint :refer [print-table] :rename {print-table print}]
            [clojure.set :as set]))

(def keywords '("SELECT", "FROM", "WHERE", "ON", "GROUP", "HAVING", "ORDER"))
(def kw_multi_arity '("SELECT", "WHERE", "ORDER", "INNER"))

(defn isKeyword
  [word]
  (some (partial = word) keywords))
(defn isMultiArityKeyword
  [word]
  (some (partial = word) kw_multi_arity))

;takes LIST, returns LIST
(defn analyze-arguments
  [input_string_splitted]
  (if (not (empty? input_string_splitted))
    (if (not (isKeyword (first input_string_splitted)))
      (if (or (= (first input_string_splitted) "AND") (= (first input_string_splitted) "OR") (= (first input_string_splitted) "BY") (= (first input_string_splitted) "DESC") (= (first input_string_splitted) "JOIN"))
        (analyze-arguments (rest input_string_splitted))
        (conj (analyze-arguments (rest input_string_splitted)) (keyword (first input_string_splitted)))
        )  ;--------change--------
      ;(analyze-arguments (rest input_string_splitted)) ; -- if uncommented, returns a LIST of all args in query, if commented - return a LIST of all args till next kw
      )
    )
  )

;takes VEC of vals, converts any integer-string into integers
(defn makeIntegersInVector
  [vector pos acc]
  (if (< pos (count vector))
    (if (and (every? #(Character/isDigit %) (get vector pos)) (not (str/blank? (get vector pos))))
      (makeIntegersInVector vector (+ 1 pos) (conj acc (Integer/parseInt (get vector pos))))
      (makeIntegersInVector vector (+ 1 pos) (conj acc (get vector pos)))
      )
    acc
    )
  )

;takes initial DF with integer-string values, returns DF with integers (where needed)
(defn makeIntegersInDF
  [initialFrame pos acc]
  (if (< pos (count initialFrame))
    (makeIntegersInDF initialFrame (+ 1 pos) (conj acc (zipmap (keys (get initialFrame pos)) (makeIntegersInVector (vec (vals (get initialFrame pos))) 0 (vector)))))
    acc
    )
  )

;takes VEC of input query, creates a map of key/value where key = SQL keyword, value = arg(s) of this keyword
(defn query-map
  [input_string_splitted map pos]
  (if (< pos (count input_string_splitted))
    (if (isMultiArityKeyword (get input_string_splitted pos))
      (query-map input_string_splitted (assoc map (get input_string_splitted pos) (analyze-arguments (subvec input_string_splitted (+ 1 pos)))) (+ 1 pos))
      (if (isKeyword (get input_string_splitted pos))
        ;(query-map input_string_splitted (assoc map "WHERE" (get input_string_splitted (+ 1 pos))) (+ 1 pos))
        (query-map input_string_splitted (assoc map (get input_string_splitted pos) (get input_string_splitted (+ 1 pos))) (+ 1 pos))
        (query-map input_string_splitted map (+ 1 pos))
        )
      )
    map
    )
  )

;takes a MAP (row), LIST (cols), returns a MAP with cols from the LIST
(defn colsFromMap
  [initMap cols resMap]
  (if (some (partial = :*) cols)
    initMap
    (if (not (empty? cols))
      (if (contains? initMap (first cols))
        (colsFromMap initMap (rest cols) (assoc resMap (first cols) (get initMap (first cols))))
        (colsFromMap initMap (rest cols) resMap))
      resMap)
    )
  )

;takes initial DF (file), 0, qMap and selectOption (SELECT / SELECT DISTINCT), returns DF with selected keys
(defn getDFwithSelCols
  [initFrame pos condList]
  (if (< pos (count initFrame))
    (if (or (some (partial = :AVG) condList) (some (partial = :MIN) condList) (some (partial = :COUNT) condList))
      initFrame
      (conj (getDFwithSelCols initFrame (+ 1 pos) condList) (colsFromMap (get initFrame pos) condList {})))
    )
  )

(defn parseColNamesIJ
  [list]
  (if (not (empty? list))
    (if (some (partial = :*) list)
      '(:*)
      (conj (parseColNamesIJ (rest list)) (keyword (get (str/split (name (first list)) #"\.") 2))))
    )
  )
;three following functions convert raw data from the file into VEC of MAPS (dataframe)
(defn readTSV [name]
  (with-open [reader (io/reader name)]
    (doall
      (csv/read-csv reader :separator \tab))))
(defn readCSV [name]
  (with-open [reader (io/reader name)]
    (doall
      (csv/read-csv reader))))
(defn rawDataToMapVec [head & lines]
  (vec (map #(zipmap (map keyword head) %1) lines)))

(defn checkFormat [name]
  (def fformat (str/split name #"\."))
  (if (= (str (nth fformat 1)) "csv") true false))
;simple printing
(defn load
  [fname]
  (if (checkFormat fname)
    (print (apply rawDataToMapVec (readCSV fname)))
    (print (apply rawDataToMapVec (readTSV fname)))))

;takes a qMap, returns a VEC with key, desirable val and comparison operator
(defn predicate
  [val1 val2]
  (if (= val2 "")
    false
    (> val2 val1)
    )
  )
(defn parseWhereCond
  [cond]
  (if (not (nil? cond))
    (if (.contains cond "=")
      (conj (str/split cond #"=") "=")
      (conj (str/split cond #">") ">")
      )
    )
  )

(defn changeCond
  [vec]
  (if (not (nil? vec))
    (if (every? #(Character/isDigit %) (get vec 1))
      (vector (keyword (get vec 0)) (Integer/parseInt (get vec 1)) (get vec 2))
      (vector (keyword (get vec 0)) (get vec 1) (get vec 2))
      )
    nil
    )
  )

(defn modWhereSingle                                        ;WEIRD
  [initFrame cond]
  (if (some (partial = "=") (changeCond (parseWhereCond (name (first cond)))))
    (filter (comp (partial = (get (changeCond (parseWhereCond (name (first cond)))) 1)) (get (changeCond (parseWhereCond (name (first cond)))) 0)) initFrame)
    (filter (comp (partial predicate (get (changeCond (parseWhereCond (name (first cond)))) 1)) (get (changeCond (parseWhereCond (name (first cond)))) 0)) initFrame)
    )
  )

(defn modWhereAnd
  [initFrame conds]
  (if (not (empty? conds))
    (if (some (partial = "=") (changeCond (parseWhereCond (name (first conds)))))
      (modWhereAnd (filter (comp (partial = (get (changeCond (parseWhereCond (name (first conds)))) 1)) (get (changeCond (parseWhereCond (name (first conds)))) 0)) initFrame) (rest conds))
      (modWhereAnd (filter (comp (partial predicate (get (changeCond (parseWhereCond (name (first conds)))) 1)) (get (changeCond (parseWhereCond (name (first conds)))) 0)) initFrame) (rest conds))
      )
    initFrame
    )
  )

(defn modWhereOr
  [initFrame conds colOfDFs]
  (if (not (empty? conds))
    (if (some (partial = "=") (changeCond (parseWhereCond (name (first conds)))))
      (modWhereOr initFrame (rest conds) (conj colOfDFs (filter (comp (partial = (get (changeCond (parseWhereCond (name (first conds)))) 1)) (get (changeCond (parseWhereCond (name (first conds)))) 0)) initFrame)))
      (modWhereOr initFrame (rest conds) (conj colOfDFs (filter (comp (partial predicate (get (changeCond (parseWhereCond (name (first conds)))) 1)) (get (changeCond (parseWhereCond (name (first conds)))) 0)) initFrame))))
    colOfDFs
    )
  )

(defn modifyDFbyWhereCond
  [initFrame qMap condList]
  (if (get qMap "AND")
    (modWhereAnd initFrame condList)
    (if (get qMap "OR")
      (flatten (modWhereOr initFrame condList (vector)))
      (if (nil? (get qMap "WHERE"))
        initFrame
        (modWhereSingle initFrame condList)
        )
      )
    )
  )

(defn getFrameKeys
  [dataframe pos]
  (if (< pos (count dataframe))
    (conj (getFrameKeys dataframe (+ 1 pos)) (keys (get dataframe pos)))
    )
  )

(defn getFrameConds
  [frameName conds]
  (if (not (empty? conds))
    (if (.contains (name (first conds)) (name frameName))
      (conj (getFrameConds (name frameName) (rest conds)) (get (str/split (name (first conds)) #"\.") 2))
      (getFrameConds (name frameName) (rest conds))
      )
    )
  )

(defn parseOnCond
  [qMap]
  (str/split (str/replace (get qMap "ON") #"=" ".") #"\.")
  )
;next two could be replaced with (clojure.str/join firstFrame joinedFrame {(keyword (get (parseOnCond qMap) 2)) (keyword (get (parseOnCond qMap) 5))})
(defn findRow
  [joinedFrame key val pos]
  (if (< pos (count joinedFrame))
    (if (= (get (get joinedFrame pos) key) val)
      (get joinedFrame pos)
      (findRow joinedFrame key val (+ 1 pos))
      )
    )
  )

(defn innerJoin
  [firstFrame joinedFrame keyFirst keyJoined pos]
  (if (< pos (count firstFrame))
    (if (not (nil? (findRow joinedFrame keyJoined (get (get firstFrame pos) keyFirst) 0)))
      (conj (innerJoin firstFrame joinedFrame keyFirst keyJoined (+ 1 pos)) (merge (findRow joinedFrame keyJoined (get (get firstFrame pos) keyFirst) 0) (get firstFrame pos)))
      (innerJoin firstFrame joinedFrame keyFirst keyJoined (+ 1 pos))
      )
    )
  )

(defn modifyDFSbyWhereConds
  [qMap]
  (if (checkFormat (get qMap "FROM"))
    (def firstFrame (modifyDFbyWhereCond (vec (makeIntegersInDF (apply rawDataToMapVec (readCSV (get qMap "FROM"))) 0 (vector))) qMap (getFrameConds (get qMap "FROM") (get qMap "WHERE"))))
    (def firstFrame (modifyDFbyWhereCond (vec (makeIntegersInDF (apply rawDataToMapVec (readTSV (get qMap "FROM"))) 0 (vector))) qMap (getFrameConds (get qMap "FROM") (get qMap "WHERE"))))
    )
  (if (checkFormat (name (first (get qMap "INNER"))))
    (def joinedFrame (modifyDFbyWhereCond (vec (makeIntegersInDF (apply rawDataToMapVec (readCSV (name (first (get qMap "INNER"))))) 0 (vector))) qMap (getFrameConds (name (first (get qMap "INNER"))) (get qMap "WHERE"))))
    (def joinedFrame (modifyDFbyWhereCond (vec (makeIntegersInDF (apply rawDataToMapVec (readTSV (name (first (get qMap "INNER"))))) 0 (vector))) qMap (getFrameConds (name (first (get qMap "INNER"))) (get qMap "WHERE"))))
    )
  (vec (innerJoin firstFrame joinedFrame (keyword (get (parseOnCond qMap) 2)) (keyword (get (parseOnCond qMap) 5)) 0))
  )

(defn modifyDFbyOrderCond
  [initFrame qMap]
  (if (get qMap "ORDER_BY")
    (if (get qMap "DESC")
      (reverse (sort-by (apply juxt (get qMap "ORDER")) initFrame))
      (sort-by (apply juxt (get qMap "ORDER")) initFrame)
      )
    initFrame)
  )

(defn countInCol
  [initFrame key pos acc]
  (if (< pos (count initFrame))
    (countInCol initFrame key (+ 1 pos) (+ acc 1))
    acc
    )
  )

(defn sumInCol
  [initFrame key pos acc]
  (if (< pos (count initFrame))
    (sumInCol initFrame key (+ 1 pos) (+ acc (get (get initFrame pos) key)))
    acc
    )
  )

(defn considerSELFunc
  [initFrame qMap]
  (if (= "MIN" (name (first (get qMap "SELECT"))))
    (vector {(str/join (vector "MIN(" (name (peek (reverse (get qMap "SELECT")))) ")")) (get (apply min-key (peek (reverse (get qMap "SELECT"))) initFrame) (peek (reverse (get qMap "SELECT"))))})
    (if (= "AVG" (name (first (get qMap "SELECT"))))
      (vector {(str/join (vector "AVG(" (name (peek (reverse (get qMap "SELECT")))) ")")) (unchecked-divide-int (sumInCol initFrame (peek (reverse (get qMap "SELECT"))) 0 0) (countInCol initFrame (peek (reverse (get qMap "SELECT"))) 0 0))})
      (if (= "COUNT" (name (first (get qMap "SELECT"))))
        (vector {(str/join (vector "COUNT(" (name (peek (reverse (get qMap "SELECT")))) ")")) (countInCol initFrame (peek (reverse (get qMap "SELECT"))) 0 0)})
        initFrame
        )
      )
    )
  )

(defn -main
  [& args]
  ;receiving a query
  (def query (read-line))
  (def query-splited (str/split query #" "))
  ;creating a map with keywords & args
  (def qMap (assoc (assoc (assoc (assoc (assoc (assoc (assoc (query-map query-splited {} 0)
                            "AND" (= true (some (partial = "AND") query-splited)))
                            "OR" (= true (some (partial = "OR") query-splited)))
                            "DISTINCT" (= true (some (partial = "DISTINCT") query-splited)))
                            "ORDER_BY" (= true (and (some (partial = "ORDER") query-splited) (some (partial = "BY") query-splited))))
                            "DESC" (= true (some (partial = "DESC") query-splited)))
                            "SEL_FUNC" (= true (or (some (partial = "AVG") query-splited) (some (partial = "MIN") query-splited) (some (partial = "COUNT") query-splited))))
                            "INNER_JOIN" (= true (some (partial = "INNER") query-splited)))
    )

  ;creating a dataframe (vec of maps) from the file (.csv or .tsv)
  (if (checkFormat (get qMap "FROM"))
    (def dataframe (vec (makeIntegersInDF (apply rawDataToMapVec (readCSV (get qMap "FROM"))) 0 (vector))))
    (def dataframe (vec (makeIntegersInDF (apply rawDataToMapVec (readTSV (get qMap "FROM"))) 0 (vector))))
    )
  ;printing the processed dataframe considering DISTINCT option and WHERE condition

  (if (get qMap "DISTINCT")
    (if (get qMap "INNER_JOIN")
      (print (modifyDFbyOrderCond (getDFwithSelCols (considerSELFunc (distinct (vec (modifyDFSbyWhereConds qMap))) qMap) 0 (parseColNamesIJ (get qMap "SELECT"))) qMap))
      (print (modifyDFbyOrderCond (getDFwithSelCols (considerSELFunc (distinct (vec (modifyDFbyWhereCond dataframe qMap (get qMap "WHERE")))) qMap) 0 (get qMap "SELECT")) qMap))
      )
    (if (get qMap "INNER_JOIN")
      (print (modifyDFbyOrderCond (getDFwithSelCols (considerSELFunc (vec (modifyDFSbyWhereConds qMap )) qMap) 0 (parseColNamesIJ (get qMap "SELECT"))) qMap))
      (print (modifyDFbyOrderCond (getDFwithSelCols (considerSELFunc (vec (modifyDFbyWhereCond dataframe qMap (get qMap "WHERE"))) qMap) 0 (get qMap "SELECT")) qMap))
      )
    )
  ;SELECT mzs.csv.title mp-posts.csv.full_name FROM mzs.csv INNER JOIN mp-posts.csv ON mzs.csv.title=mp-posts.csv.full_name
  )