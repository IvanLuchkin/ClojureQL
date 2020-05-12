(ns core.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.pprint :refer [print-table] :rename {print-table p}]
            [clojure.set :as set]))

(def keywords '("SELECT", "FROM", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER"))
(def kw_multi_arity '("SELECT", "WHERE", "ORDER"))

(defn isKeyword
  [word]
  (some (partial = word) keywords))
(defn isMultiArityKeyword
  [word]
  (some (partial = word) kw_multi_arity))

;takes LIST of VEC of input query, returns a LIST of all the SQL keywords in query
(defn analyze-keywords
  [input_string_splitted]
  (if (not (empty? input_string_splitted))
    (if (isKeyword (first input_string_splitted))
      (conj (analyze-keywords (rest input_string_splitted)) (first input_string_splitted))
      (analyze-keywords (rest input_string_splitted)))))

;takes LIST, returns LIST
(defn analyze-arguments
  [input_string_splitted]
  (if (not (empty? input_string_splitted))
    (if (not (isKeyword (first input_string_splitted)))
      (if (or (= (first input_string_splitted) "AND") (= (first input_string_splitted) "OR") (= (first input_string_splitted) "BY") (= (first input_string_splitted) "DESC"))
        (analyze-arguments (rest input_string_splitted))
        (conj (analyze-arguments (rest input_string_splitted)) (keyword (first input_string_splitted)))
        )  ;--------change--------
      ;(analyze-arguments (rest input_string_splitted)) ; -- if uncommented, returns a LIST of all args in query, if commented - return a LIST of all args till next kw
      )
    )
  )

;takes VEC of vals, converts any integer-string into integers
(defn makeIntegersInVector
  [vector pos]
  (if (< pos (count vector))
    (if (and (every? #(Character/isDigit %) (get vector pos)) (not (str/blank? (get vector pos))))
      (conj (makeIntegersInVector vector (+ 1 pos)) (Integer/parseInt (get vector pos)))
      (conj (makeIntegersInVector vector (+ 1 pos)) (get vector pos))
      )
    )
  )

;takes initial DF with integer-string values, returns DF with integers (where needed)
(defn makeIntegersInDF
  [initialFrame pos]
  (if (< pos (count initialFrame))
    (conj (makeIntegersInDF initialFrame (+ 1 pos)) (zipmap (keys (get initialFrame pos)) (makeIntegersInVector (vec (vals (get initialFrame pos))) 0))))
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
  [initFrame pos qMap]
  (if (< pos (count initFrame))
    (if (or (some (partial = :AVG) (get qMap "SELECT")) (some (partial = :MIN) (get qMap "SELECT")) (some (partial = :COUNT) (get qMap "SELECT")))
      initFrame
      (conj (getDFwithSelCols initFrame (+ 1 pos) qMap) (colsFromMap (get initFrame pos) (get qMap "SELECT") {})))
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
  (vec (map #(zipmap (map keyword head) %1) lines)))        ;------change------

(defn checkFormat [name]
  (def fformat (str/split name #"\."))
  (if (= (str (nth fformat 1)) "csv") true false))
;simple printing
(defn load
  [fname]
  (if (checkFormat fname)
    (p (apply rawDataToMapVec (readCSV fname)))
    (p (apply rawDataToMapVec (readTSV fname)))))

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
  [initFrame qMap]
  (if (get qMap "AND")
    (modWhereAnd initFrame (get qMap "WHERE"))
    (if (get qMap "OR")
      (flatten (modWhereOr initFrame (get qMap "WHERE") (vector)))
      (if (nil? (get qMap "WHERE"))
        initFrame
        (modWhereSingle initFrame (get qMap "WHERE"))
        )
      )
    )
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
  (def qMap (assoc (assoc (assoc (assoc (assoc (assoc (query-map query-splited {} 0)
                            "AND" (= true (some (partial = "AND") query-splited)))
                            "OR" (= true (some (partial = "OR") query-splited)))
                            "DISTINCT" (= true (some (partial = "DISTINCT") query-splited)))
                            "ORDER_BY" (= true (and (some (partial = "ORDER") query-splited) (some (partial = "BY") query-splited))))
                            "DESC" (= true (some (partial = "DESC") query-splited)))
                            "SEL_FUNC" (= true (or (some (partial = "AVG") query-splited) (some (partial = "MIN") query-splited) (some (partial = "COUNT") query-splited))))
    )

  ;creating a dataframe (vec of maps) from the file (.csv or .tsv)
  (if (checkFormat (get qMap "FROM"))
    (def dataframe (vec (makeIntegersInDF (apply rawDataToMapVec (readCSV (get qMap "FROM"))) 0)))
    (def dataframe (vec (makeIntegersInDF (apply rawDataToMapVec (readTSV (get qMap "FROM"))) 0)))
    )
  ;printing the processed dataframe considering DISTINCT option and WHERE condition

  (if (get qMap "DISTINCT")
    (p (modifyDFbyOrderCond (getDFwithSelCols (considerSELFunc (distinct (vec (modifyDFbyWhereCond dataframe qMap))) qMap) 0 qMap) qMap))
    (p (modifyDFbyOrderCond (getDFwithSelCols (considerSELFunc (vec (modifyDFbyWhereCond dataframe qMap)) qMap) 0 qMap) qMap))
    )
  (-main)
  )