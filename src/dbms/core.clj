(ns dbms.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.data.csv :as reader]
            [clojure.pprint :refer [print-table] :rename {print-table p}]
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


(defn getFileFormat [parsedSql]
  (nth (str/split (get parsedSql :tableName) #"\.") 1)
  )

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
                      (empty? %) ()
                      :else
                      %
                      ) va))
  )

(defn checkEmptyOneRow [oneRow]
  (into {} (filter #(not (empty? (last %))) oneRow))
  )

(defn formate-delete-empty [table]
  (into () (map #(checkEmptyOneRow %) table))
  )


(defn formateTable [table]
  (into () (map #(checkOneRow %) table))
  )


(defn preCreateTable [name format]
  (doall (apply formate-data-to-hashmap (saveSeq name format)))
  )

(defn createTable [name format]
  (formateTable (formate-delete-empty (preCreateTable name format)))
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


(defn getKeys [table]
  ((comp distinct flatten) (map #((comp keys first) %) [table]))
  )


(defn getOneRowAllAttributes [row headers]
  (concat (filterv #(not (contains? headers %)) headers))
  )


(defn getAllTableAttribute [table]
  (into [] (distinct (flatten (merge (mapv #(keys %) table)
                                     ))))
  )

;----------------------------------------------------------Distinct
(defn myDistinct [table]
  (into [] (doall (distinct table))))


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


(defn findHavingExpression [query]
  (def firstInd (+ (.indexOf query "having") 7))
  (def lastInd (+ (.lastIndexOf query ")") 1))
  (subs query firstInd lastInd)
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

(defn executeBetweensForHaving [splitedWhereClause file]
  (def valuesForBetween (myBetweenValues (get splitedWhereClause 1)))
  (def word (first splitedWhereClause))
  (def colForBetween (str (subs word 0 (.indexOf word "(")) (subs word (+ (.indexOf word "(") 1) (.indexOf word ")")))
    )
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
  (def splitedArrayByLogicalValues (doall (findAllConditionInWhere (findWhereExpression query))))
  (doall (mapv #(cond
                  (.contains (str/split % #" ") "not") (executeNotBetweens (str/split (str/trim (str/replace % #"not" "")) #" ") file)
                  :else
                  (executeBetweens (str/split % #" ") file)

                  ) splitedArrayByLogicalValues))
  )


(defn createArrayOfDfInHaving [query file]
  (def splitedArrayByLogicalValues (findAllConditionInWhere (findHavingExpression query)))
  (doall (mapv #(cond
                  (.contains (str/split % #" ") "not") (executeNotBetweens (str/split (str/trim (str/replace % #"not" "")) #" ") file)
                  :else
                  (executeBetweensForHaving (str/split % #" ") file)

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


(defn recursiveConcatHaving [finAr arrayOfDf arrayOfCond]
  (if (and (= (count finAr) 0) (= (count arrayOfDf) 1) (empty? arrayOfCond)) (nth arrayOfDf 0) (cond
                                                                                                 (= (empty? arrayOfCond) true) (vec finAr)
                                                                                                 (= (empty? finAr) true) (let [kek (nth arrayOfCond 0)]
                                                                                                                           (case kek
                                                                                                                             "and" (recursiveConcatHaving (mergeAndConditionTwoDf (nth arrayOfDf 0) (nth arrayOfDf 1)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
                                                                                                                             "or" (recursiveConcatHaving (mergeOrConditiontTwoDf (nth arrayOfDf 0) (nth arrayOfDf 1)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
                                                                                                                             )
                                                                                                                           )
                                                                                                 :else (let [choose (first arrayOfCond)]
                                                                                                         (case choose
                                                                                                           "and" (recursiveConcatHaving (mergeAndConditionTwoDf finAr (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
                                                                                                           "or" (recursiveConcatHaving (mergeOrConditiontTwoDf finAr (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
                                                                                                           )
                                                                                                         )
                                                                                                 ))
  )




;------------------------------------------------------------Select Columns
(defn selectColumn [col file]
  (cond
    (= (get col 0) "*") (mapv #(select-keys % (getAllTableAttribute file)) file)
    :else
    (mapv #(select-keys % col) file)
    ))


(defn getTableIndex [query]
  (+ (.indexOf query "from") 1))

(defn parseCol [query]
  (str/split (get query 1) #","
             ))

(defn getJoinsClause [query]
  (filterv #(or (= (compare "outter" %) 0) (= (compare "right" %) 0) (= (compare "inner" %) 0)) query)
  )
(defn parseJoinsOnExpr [query]
  (subvec query (+ (.indexOf query "on") 1) (+ (.indexOf query "on") 4))
  )

(defn getColonsForJoins [expr]
  (def firs (last (str/split (first expr) #"\.")))
  (def sec (last (str/split (last expr) #"\.")))
  [firs sec]
  )
(defn getJoinedTable [query]
  (subvec query (+ (.indexOf query "join") 1) (+ (.indexOf query "join") 2))
  )


;---------------------------------------------------------------Parse GroupBy params

(defn findArgs [query]
  (str/split (nth query (+ (.indexOf query "group") 2)) #",")
  )

;--------------------------------------------------Find Having
(defn findHaving [query]
  (.indexOf query "having")
  )






(defn parseSqlQuery [query]
  (cond
    (some? (some (partial = "select") query)) (if (some? (some (partial = "distinct") query)) (conj {:isSelect true}
                                                                                                    (parseSqlQuery (into [] (subvec query 1)))) (conj {:isSelect true}
                                                                                                                                                      (parseSqlQuery (into [] (concat ["exp"] (subvec query 1))))))
    (some? (some (partial = "distinct") query)) (conj {:isDistinct true}
                                                      (parseSqlQuery (into [] (concat ["exp"] (subvec query 1)))))
    (some? (some (partial = "exp") query)) (conj {:expressions (parseCol query)}
                                                 (parseSqlQuery (subvec query 1)))
    (some? (some (partial = "group") query)) (conj {:groupBy true :groupByParams (findArgs query)}
                                                   (parseSqlQuery
                                                     (into [] (concat (subvec query 0 (.indexOf query "group"))
                                                                      (subvec query (+ (.indexOf query "group") 2))))))
    (some? (some (partial = "having") query)) (conj {:isHaving true}
                                                    (parseSqlQuery
                                                      (into [] (concat (subvec query 0 (.indexOf query "having"))
                                                                       (subvec query (+ (.indexOf query "having") 1))
                                                                       ))))
    (some? (some (partial = "from") query)) (conj {:tableName (get query (getTableIndex query))}
                                                  (parseSqlQuery (into [] (concat (subvec query 0 (- (getTableIndex query) 1)) (subvec query (+ (getTableIndex query) 1))))))
    (some? (some (partial = "where") query)) (conj {:isWhere true}
                                                   (parseSqlQuery (into [] (concat (subvec query 0 (.indexOf query "where")) (subvec query (+ (.indexOf query "where") 1))))))
    (some? (some (partial = "order") query)) (conj {:isOrderBy true}
                                                   (parseSqlQuery (into [] (concat (subvec query 0 (.indexOf query "order")) (subvec query (+ (.indexOf query "by") 1))))))
    (some? (some (partial = "join") query)) (conj {:isJoin true :joinType (getJoinsClause query) :joinedTable (getJoinedTable query) :valsJoin (getColonsForJoins (parseJoinsOnExpr query))}
                                                  (parseSqlQuery (into [] (concat (subvec query 0 (+ (getTableIndex query) 1))
                                                                                  (subvec query (+ (getTableIndex query) 3))
                                                                                  ))))

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





(defn myCount [table col]
  (if (= (compare col "*") 0)
    (count table)
    (count (filterv #(= (count (vals (select-keys % [col]))) 1) table))
    ))

;-----------------------------------------------------------Agregate fucntions


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


(defn printAgregationsFuncResult [func res]
  (apply println func)
  (apply println res)
  )

;--------------------------------------------------------------Joins

(defn innerJoin [ds1 ds2 col1 col2]
  (into [] (clojure.set/join ds1 ds2 {col1 col2}))
  )

(defn outterJoin [ds1 ds2 col1 col2]
  (let [z1 (zipmap (mapv col1 ds1) ds1)
        z2 (zipmap (mapv col2 ds2) ds2)
        ]
    (vals (merge-with merge z1 z2)))
  )

(defn rightJoin [ds1 ds2 col1 col2]
  (def out (outterJoin ds1 ds2 col1 col2))
  (into [] (filterv #(some (partial = (first (vals %))) (map col2 ds2)) out))
  )

(defn joinExecuition [table1 table2 col1 col2 joinOption]
  (case joinOption
    "inner" (to-string-map (innerJoin (to-keyword-map table1) (to-keyword-map table2) (keyword col1) (keyword col2)))
    "outter" (to-string-map (outterJoin (to-keyword-map table1) (to-keyword-map table2) (keyword col1) (keyword col2)))
    "right" (to-string-map (rightJoin (to-keyword-map table1) (to-keyword-map table2) (keyword col1) (keyword col2)))
    )
  )




;---------------sum(money) -> [sum , money]

(defn findParams [word]
  (def option (subs word 0 (.indexOf word "(")))
  (def va (keyword (subs word (+ (.indexOf word "(") 1) (.indexOf word ")"))))
  [option va]
  )



;-----------------------------------------------------------Find all agregate func in select without columns.
;------------------Args - array of query
(defn findAgregate [query]
  (filterv #(not= (.indexOf % "(") -1) query)
  )

;----------------Finds All columns in select query
;------------------Args - array of query
(defn findColumnsInQuery [query]
  (filterv #(= (.indexOf % "(") -1) query)
  )

;--------------------------------------------------------------GroupBy


(defn myGroupByEx [table groupedCols valss ar]
  (if (empty? valss) ar
                     (let [elems (findParams (first valss))
                           ]
                       (def tabl (group-by #(select-keys % groupedCols) (doall table)))
                       (def tab (mapv (fn [[grp-map values]] (let [option (first elems)
                                                                   valFor (last elems)
                                                                   ]
                                                               (case option
                                                                 "sum" (assoc grp-map (keyword (str "sum" (subs (str valFor) 1))) (reduce + (map valFor values)))
                                                                 "count" (assoc grp-map (keyword (str "count" (subs (str valFor) 1))) (count (map valFor values)))
                                                                 )
                                                               )
                                        ) tabl))
                       (if (empty? ar) (myGroupByEx table groupedCols (subvec valss 1) tab)
                                       (myGroupByEx table groupedCols (subvec valss 1) (mapv #(conj %1 %2) ar tab)))
                       )
                     )
  )

(defn selectColumsInGroupBy [table columns groupByArgs]
  (if (empty? groupByArgs) table (if (some (partial = (first groupByArgs)) columns)
                                   (selectColumsInGroupBy table columns (subvec groupByArgs 1))
                                   (selectColumsInGroupBy (mapv #(dissoc % (first groupByArgs)) table) columns (subvec groupByArgs 1))
                                   ))
  )


(defn executeSqlQuery []
  (def query (read-line))
  (def splitedLine (str/split query #" "))
  (def parsedSql (parseSqlQuery splitedLine))
  (def tabl (if (contains? parsedSql :isJoin)
              ;True
              (joinExecuition (createTable (get parsedSql :tableName) (getFileFormat parsedSql))
                              (createTable (first (get parsedSql :joinedTable)) (getFileFormat parsedSql))
                              (first (get parsedSql :valsJoin))
                              (last (get parsedSql :valsJoin))
                              (first (get parsedSql :joinType))
                              )
              ;False
              (createTable (get parsedSql :tableName) (nth (str/split (get parsedSql :tableName) #"\.") 1))
              ))
  (def initialTable (if (and (contains? parsedSql :isSelect) (contains? parsedSql :tableName))
                      (selectColumn (getAllTableAttribute tabl) tabl)
                      (print "error in query")))
  (def tableWithWhere (if (contains? parsedSql :isWhere) (recursiveConcat [] (createArrayOfDfInWhere query tabl) (findAllClausesInWhere (findWhereExpression query))) initialTable))
  (def tableWithGroupBy (if (contains? parsedSql :groupBy)
                          (to-string-map(selectColumsInGroupBy(myGroupByEx (to-keyword-map tableWithWhere) (mapv #(keyword %) (get parsedSql :groupByParams)) (findAgregate (get parsedSql :expressions)) []) (mapv #(keyword %) (findColumnsInQuery(get parsedSql :expressions)))(mapv #(keyword %) (get parsedSql :groupByParams))))
                          (if (and (contains? parsedSql :expressions) (not= (.indexOf (first (get parsedSql :expressions)) "(") -1))
                            ((def funcsWithArgs (recursiveParsingAgregateFunctionsIntoMap (get parsedSql :expressions) []))
                             (def valuesOfExecution (executeAgregatesFunc tabl (unboundKeys funcsWithArgs) (unboundVals funcsWithArgs) []))
                             (def arrayOfFunctions (unboundKeys funcsWithArgs))
                             (printAgregationsFuncResult arrayOfFunctions valuesOfExecution)
                             (executeSqlQuery))
                            (selectColumn (get parsedSql :expressions) tableWithWhere))
                          ))
  (def tableWithHaving (if (and(contains? parsedSql :isHaving) (contains? parsedSql :groupBy))
                         (recursiveConcatHaving [] (createArrayOfDfInHaving query tableWithGroupBy) (findAllClausesInWhere (findHavingExpression query)))
                         tableWithGroupBy
                         ))

  (def tableWithDistinct (if (contains? parsedSql :isDistinct) (myDistinct tableWithHaving) tableWithHaving))

  (def tableWithOrderBy (if (contains? parsedSql :isOrderBy)
                          (p (getAllTableAttribute tableWithDistinct) (to-string-map (executeOrderByOptional (getOrderByClause splitedLine)
                                                                                                             (to-keyword-map tableWithDistinct)
                                                                                                             (mapv #(keyword %) (getOrderByCols splitedLine)))))
                          (p (getAllTableAttribute tableWithDistinct) tableWithDistinct)))
  (executeSqlQuery)
  )