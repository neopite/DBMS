(ns dbms.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.data.csv :as reader]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.set :as set]))

(defn saveSeq [name format]
  (cond
    (= format "tsv") (with-open [reader (io/reader name)]
                       (doall
                         (reader/read-csv reader :separator \tab)))
    (= format "csv") (with-open [reader (io/reader name)]
                       (doall
                         (reader/read-csv reader)))))



(defn formate-data-to-hashmap [heads & info]
  (def headers heads)
  (map #(zipmap headers %) info))

;This func check is a str is a number
;This func is used for creating table(formatting numbers in string to integer type)
(defn checkIfStringIsNumber [str]
  (if (empty? str) false (every? #(Character/isDigit %) str))
  )

(defn checkOneRow [oneRow]
  (def va (vals oneRow))
  (def ke (keys oneRow))
  (zipmap ke (mapv #(cond
                      (checkIfStringIsNumber %) (Integer/parseInt %)
                      (empty? %) 0
                      :else
                      %
                      ) va))
  )


(defn formateTable [table]
  (into () (map #(checkOneRow %) table))
  )


(defn createTable [name format]
  (formateTable (doall (apply formate-data-to-hashmap (saveSeq name format))))
  )


(defn printData [table]
  (loop [outter 0]
    (when (< outter (count table))
      (loop [inner 0]
        (when (< inner (count (nth table outter)))
          (print (format "%30s  ||  " (nth (vals (nth table outter)) inner)))
          (recur (inc inner))))
      (println "")
      (recur (inc outter))))
  )

(def tab (createTable "mp-assistants.csv" "csv"))

(defn printHeaders [headers]
  (loop [itter 0]
    (when (< itter (count headers))
      (print (format "%30s  ||  " (name (nth (keys headers) itter))))
      (recur (inc itter))))
  (println)
  )



(defn print-formated-hashmap-in-table [table]
  (printHeaders (first table))
  (printData table)
  )


(defn getKeys [file]
  (keys (first file)))


;----------------------------------------------------------Distinct
(defn myDistinct [table]
  (into [] (distinct table)))


;@param - where query without where(not splitted)
;find all 'and' and 'or' in order
;@return - array of logical values
(defn findAllClausesInWhere [whereQuery]
  (def splited (str/split whereQuery #" "))
  (filterv (fn [ar] (or (= 0 (compare ar "and")) (= 0 (compare ar "or")))) splited)
  )

;@param - where query without where (only conditions)
;find all condition except logical values
;@return - array of conditions

(defn findAllConditionInWhere [whereQuery]
  (def splited (str/split whereQuery #"and|or"))
  (map #(str/trim %) splited)
  )



(defn findWhereExpression [query]
  (def firstInd (+ (.indexOf query "where") +6))
  (def lastInd (+ (.lastIndexOf query ")") 1))              ;;rewrite finder !-1!_!_!_!_!_!_!_!!__!_!_!_!!_!_!_!_!_!_!_!_!_!_!
  (subs query firstInd lastInd))

(defn splitWhereClause [whereClause]
  (str/split whereClause #" ")
  )

(defn myBetweenValues [betweenExpr]
  (def firs (+ (int (.indexOf betweenExpr "(")) 1))
  (def secon (int (.indexOf betweenExpr ")")))
  (str/split (subs betweenExpr firs secon) #","))

;---------------------------------------------------------------Between(value,value)
(defn betweens [valuess file col]
  (filterv (fn [line] (and (< (get (select-keys line [col]) col) (Integer/parseInt (nth valuess 1)))
                           (> (get (select-keys line [col]) col) (Integer/parseInt (nth valuess 0)))
                           ))
           file))
(defn executeBetweens [splitedWhereClause file]
  (def valuesForBetween (myBetweenValues (get splitedWhereClause 1)))
  (def colForBetween (get splitedWhereClause 0))
  (betweens valuesForBetween file colForBetween)
  )
;----------------------------------------------------------------not between(value,value);
(defn notBetweens [valuess file col]
  (filterv (fn [line] (or (> (get (select-keys line [col]) col) (Integer/parseInt (nth valuess 1)))
                          (< (get (select-keys line [col]) col) (Integer/parseInt (nth valuess 0)))
                          ))
           file))


;-----------------------------------------------------------Merging two maps into one with different condition
(defn mergeAndConditionTwoDf [df1 df2]
  (filterv (fn [x] (.contains df2 x)) df1))

(defn mergeOrConditiontTwoDf [df1 df2]
  (myDistinct (set/union df1 df2))
  )
;------------------------------------------------------------Not between execution



(defn executeNotBetweens [splitedWhereClause file]
  (def valuesForBetween (myBetweenValues (get splitedWhereClause 1)))
  (def colForBetween (get splitedWhereClause 0))
  (notBetweens valuesForBetween file colForBetween)
  )


(defn createArrayOfDfInWhere [query file]
  (def splitedArrayByLogicalValues (findAllConditionInWhere (findWhereExpression query)))
  (doall (mapv #(cond
                  (.contains (str/split % #" ") "not") (executeNotBetweens (str/split (str/trim (str/replace % #"not" "")) #" ") file)
                  :else
                  (executeBetweens (str/split % #" ") file)

                  ) splitedArrayByLogicalValues))
  )
;--------------------------------------------------------------------Calculate final Df for where clauses

(defn recursiveConcat [finAr arrayOfDf arrayOfCond]
  (if (and (= (count finAr) 0) (= (count arrayOfDf) 1) (empty? arrayOfCond)) (nth arrayOfDf 0) (cond
                                                                                                 (= (empty? arrayOfCond) true) (vec finAr)
                                                                                                 (= (empty? finAr) true) (let [kek (nth arrayOfCond 0)]
                                                                                                                           (case kek
                                                                                                                             "and" (recursiveConcat (mergeAndConditionTwoDf (nth arrayOfDf 0) (nth arrayOfDf 1)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
                                                                                                                             "or" (recursiveConcat (mergeOrConditiontTwoDf (nth arrayOfDf 0) (nth arrayOfDf 1)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
                                                                                                                             )
                                                                                                                           )
                                                                                                 :else (let [choose (first arrayOfCond)]
                                                                                                         (case choose
                                                                                                           "and" (recursiveConcat (mergeAndConditionTwoDf finAr (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
                                                                                                           "or" (recursiveConcat (mergeOrConditiontTwoDf finAr (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
                                                                                                           )
                                                                                                         )
                                                                                                 ))
  )



;------------------------------------------------------------Select Columns
(defn selectColumn [col file]
  (cond
    (= (get col 0) "*") (map #(select-keys % (getKeys file)) file)
    :else
    (map #(select-keys % col) file)

    ))


(defn getTableIndex [query]
  (+ (.indexOf query "from") 1))

(defn parseCol [query]
  (str/split (get query 1) #","
             ))

(defn parseSqlQuery [query]
  (cond
    (some? (some (partial = "select") query)) (if (some? (some (partial = "distinct") query))
                                                (conj {:isSelect true}
                                                      (parseSqlQuery (into [] (subvec query 1))))
                                                (conj {:isSelect true}
                                                      (parseSqlQuery (into [] (concat ["nextexps"] (subvec query 1))))))
    (some? (some (partial = "distinct") query)) (conj {:isDistinct true}
                                                      (parseSqlQuery (into [] (concat ["nextexps"] (subvec query 1)))))
    (some? (some (partial = "nextexps") query)) (conj {:expressions (parseCol query)}
                                                      (parseSqlQuery (subvec query 1)))
    (some? (some (partial = "from") query)) (conj {:tableName (get query (getTableIndex query))}
                                                  (parseSqlQuery (into [] (concat (subvec query 0 (- (getTableIndex query) 1))
                                                                                  (subvec query (getTableIndex query))))))
    (some? (some (partial = "where") query)) (conj {:isWhere true}
                                                   (parseSqlQuery (into [] (concat (subvec query 0 (.indexOf query "where"))
                                                                                   (subvec query (+ (.indexOf query "where") 1))))))
    (some? (some (partial = "order") query)) (conj {:isOrderBy true}
                                                   (parseSqlQuery (into [] (concat (subvec query 0 (.indexOf query "order"))
                                                                                   (subvec query (+ (.indexOf query "by") 1))
                                                                                   )))
                                                   )

    :else {}))


(defn to-string-keys
  [table]
  (zipmap (mapv (comp name name) (keys table)) (vals table)))

(defn to-string-map
  [table]
  (mapv #(to-string-keys %) table)
  )

(defn to-keyword-keys
  [table]
  (zipmap (mapv (comp keyword name) (keys table)) (vals table)))

(defn to-keyword-map
  [table]
  (mapv #(to-keyword-keys %) table)
  )

;---------------------------------------------------------------------------------------ORDER BY
(defn getOrderByClause [query]
  (filterv #(or (= (compare "asc" %) 0) (= (compare "desc" %) 0)) query)
  )
(defn getOrderByCols [query]
  (str/split (nth query (+ (.indexOf query "order") 2)) #",")
  )

(defn myOrderByAsc [table cols]
  (sort-by (apply juxt cols) table)
  )

(defn myOrderByDesc [table cols]
  (reverse (sort-by (apply juxt cols) table))
  )

(defn executeOrderByOptional [option table cols]
  (if (= (compare (nth option 0) "asc") 0) (myOrderByAsc table cols) (myOrderByDesc table cols))
  )


;-----------------------------------------------------------Agregate fucntions



(defn myCount [table col]
  (if (= (compare col "*") 0)
    (count (filterv #(not= (nth (vals (select-keys % [(getKeys table)])) 0) 0) table))
    (count (filterv #(not= (nth (vals (select-keys % [col])) 0) 0) table))
    )
  )

(defn myMedian [table col]
  (def oneCol (to-string-map (executeOrderByOptional ["asc"] (to-keyword-map (selectColumn [col] table)) (mapv #(keyword %) [col]))))
  (if (= (rem (count oneCol) 2) 1) (nth (vals (nth oneCol (int (Math/floor (/ (count oneCol) 2))))) 0)
                                   (double
                                     (/
                                       (+
                                         (first
                                           (vals
                                             (nth oneCol
                                                  (/ (count oneCol) 2))))
                                         (first (vals (nth oneCol (- (/ (count oneCol) 2) 1))))) 2)))

  )


(defn mySum [table col]
  (nth (vals (apply merge-with + (selectColumn [col] table))) 0)
  )

;------------------------------------------------------------Execution agregate fucntion
(defn executeAgregatesFunc [table funcs args total]
  (if (= (count funcs) 0) total
                          (let [choose (nth funcs 0)]
                            (case choose
                              "count" (executeAgregatesFunc table (subvec funcs 1) (subvec args 1) (conj total (myCount table (nth args 0))))
                              "sum" (executeAgregatesFunc table (subvec funcs 1) (subvec args 1) (conj total (mySum table (nth args 0))))
                              "med" (executeAgregatesFunc table (subvec funcs 1) (subvec args 1) (conj total (myMedian table (nth args 0))))
                              )
                            )
                          )
  )





;Return entry of - agregateFucn and col => {"count" "*"}
;---------------------------------------------------------Agregate Functions Parsing

(defn parseOneAgragateIntoEntry [expr]
  (def indexBetweenFucnAndBound (.indexOf expr "("))
  (def col (subs expr (+ indexBetweenFucnAndBound 1) (.indexOf expr ")")))
  (def func (subs expr 0 indexBetweenFucnAndBound))
  {func col}
  )


;Return the vector of maps withs parsed expresion [{"count" "row}{"sum" "row"}]
(defn recursiveParsingAgregateFunctionsIntoMap [expr total]
  (if (empty? expr) total
                    (recursiveParsingAgregateFunctionsIntoMap (subvec expr 1) (conj total (parseOneAgragateIntoEntry (first expr))))
                    )
  )


;-----------------------------------------------------------------------Get Keys from mapEntry and Unbound from brackets elements in vector[{ "count" "*"  "sum" "row"}] => [count sum]

(defn unboundKeys [arr]
  (into [] (map (fn [arg]
                  (first (keys arg))
                  ) arr
                ))
  )

;;Also as for vals

(defn unboundVals [arr]
  (into [] (map (fn [arg]
                  (first (vals arg))
                  ) arr
                ))
  )


(defn printResult [func res]
  (apply println func)
  (apply println res)
  )


(defn executeSqlQuery []
  (def query (read-line))
  (def splitedLine (str/split query #" "))
  (def parsedSql (parseSqlQuery splitedLine))
  (def tabl (createTable (get parsedSql :tableName) (nth (str/split (get parsedSql :tableName) #"\.") 1)))
  (def initialTable (if (and (contains? parsedSql :isSelect) (contains? parsedSql :tableName))
                      (selectColumn (getKeys tabl) tabl)
                      (print "error in query")))
  (def tableWithWhere (if (contains? parsedSql :isWhere) (recursiveConcat [] (createArrayOfDfInWhere query tabl) (findAllClausesInWhere (findWhereExpression query))) initialTable))

  (def tableWithAgregatesFunctions (if (and (contains? parsedSql :expressions) (not= (.indexOf (first (get parsedSql :expressions)) "(") -1))
                                     (
                                      (def funcsWithArgs (recursiveParsingAgregateFunctionsIntoMap (get parsedSql :expressions) []))
                                      (def valuesOfExecution (executeAgregatesFunc tabl (unboundKeys funcsWithArgs) (unboundVals funcsWithArgs) []))
                                      (def arrayOfFunctions (unboundKeys funcsWithArgs))
                                      (printResult arrayOfFunctions valuesOfExecution)
                                      (executeSqlQuery))
                                     tableWithWhere
                                     ))
  (def tableWithDistinct (if (contains? parsedSql :isDistinct) (myDistinct tableWithAgregatesFunctions) tableWithAgregatesFunctions))

  (def tableWithOrderBy (if (contains? parsedSql :isOrderBy)
                          (to-string-map (executeOrderByOptional (getOrderByClause splitedLine)
                                                                 (to-keyword-map tableWithDistinct)
                                                                 (mapv #(keyword %) (getOrderByCols splitedLine))))
                          tableWithDistinct))
  (if (contains? parsedSql :expressions) (print-formated-hashmap-in-table (selectColumn (get parsedSql :expressions) tableWithOrderBy)) (print-formated-hashmap-in-table tableWithOrderBy))
  (executeSqlQuery)
  )


