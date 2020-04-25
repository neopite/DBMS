(ns dbms.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.data.csv :as reader]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.set :as set]))

(defn readFile [name]
  (with-open [reader (io/reader name)]
    (doall
      (reader/read-csv reader))))

(defn formate-data-to-hashmap [heads & info]
  (def headers heads)
  (map #(zipmap headers %) info))

(defn createTable [name]
  (doall(apply formate-data-to-hashmap (readFile name))))

(defn printData [table]
  (loop [outter 0]                                          ;;Внешний цикл по записям от 0 ... n;
    (when (< outter (count table))                          ;; Проверяет количество пройденых записей по hashmap
      (loop [inner 0]                                       ;;Внутренний цикл по каждой колонке n записи
        (when (< inner (count (nth table outter)))          ;;Проверка на число колонок которое уже пройдено
          (print (format "%30s  ||  " (nth (vals (nth table outter)) inner))) ;;Вывод на экран
          (recur (inc inner))))
      (println "")
      (recur (inc outter))))
  )

(def tab (createTable "mp-assistants.csv"))

(defn printHeaders [headers]
  (loop [itter 0]
    (when (< itter (count headers))                         ;;Проверка на количество пройденых столбиков
      (print (format "%30s  ||  " (name (nth (keys headers) itter)))) ;; Вывод название колонок в формате :row  :col :pos_x :pos_y :title :id_mp :id_fr
      (recur (inc itter))))
  (println)
  )
(defn print-formated-hashmap-in-table [table]
  (printHeaders (first table))
  (printData table)
  )
(defn getKeys [file]
  (keys (first file)))

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
  (def lastInd (+ (.lastIndexOf query ")") 1))
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
  (filterv (fn [line] (and (< (Integer/parseInt (get (select-keys line [col]) col)) (Integer/parseInt (nth valuess 1)))
                           (> (Integer/parseInt (get (select-keys line [col]) col)) (Integer/parseInt (nth valuess 0)))
                           ))
           file))
(defn executeBetweens [splitedWhereClause file]
  (def valuesForBetween (myBetweenValues (get splitedWhereClause 1)))
  (def colForBetween (get splitedWhereClause 0))
  (betweens valuesForBetween file colForBetween)
  )
;----------------------------------------------------------------not between(value,value);
(defn notBetweens [valuess file col]
  (filterv (fn [line] (or (> (Integer/parseInt (get (select-keys line [col]) col)) (Integer/parseInt (nth valuess 1)))
                          (< (Integer/parseInt (get (select-keys line [col]) col)) (Integer/parseInt (nth valuess 0)))
                          ))
           file))


;-----------------------------------------------------------Merging two maps into one with different condtion
(defn mergeAndConditionTwoDf [df1 df2]
  (filterv (fn [x] (.contains df2 x)) df1))

(defn mergeOrConditiontTwoDf [df1 df2]
  (set/union df1 df2)
  )
;--------------------------------------------------------------------------------------------------


()
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

(defn recursiveConcat [finAr arrayOfDf arrayOfCond]
  (if (empty? arrayOfCond) true)
  (map #(select-keys % [(getKeys finAr)]) finAr)
  ((if (empty? finAr) true)
   (let [kek (nth arrayOfDf 0)]
     (case kek
       "and" (recursiveConcat (mergeAndConditionTwoDf (nth 0 arrayOfDf) (nth 1 arrayOfDf)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
       "or" (recursiveConcat (mergeOrConditiontTwoDf (nth 0 arrayOfDf) (nth 1 arrayOfDf)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
       )
     )
   (let [choose (first arrayOfCond)]
     (case choose
       "and" (recursiveConcat (mergeAndConditionTwoDf (finAr) (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
       "or" (recursiveConcat (mergeOrConditiontTwoDf (finAr) (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
       )
     )

   ))

(defn recursiveConcat [finAr arrayOfDf arrayOfCond]
  (cond
    (= (empty? arrayOfCond) true) (vec finAr)
    (= (empty? finAr) true) (let [kek (nth arrayOfCond 0)]
                              (case kek
                                "and" (recursiveConcat (mergeAndConditionTwoDf (nth arrayOfDf 0) (nth arrayOfDf 1)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
                                "or" (recursiveConcat (mergeOrConditiontTwoDf (nth arrayOfDf 0) (nth arrayOfDf 1)) (subvec arrayOfDf 2) (subvec arrayOfCond 1))
                                )
                              )
    :else  (let [choose (first arrayOfCond)]
             (case choose
               "and" (recursiveConcat (mergeAndConditionTwoDf finAr (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
               "or" (recursiveConcat (mergeOrConditiontTwoDf finAr (first arrayOfDf)) (subvec arrayOfDf 1) (subvec arrayOfCond 1))
               )
             )
    )
  )


;----------------------------------------------------------Distinct
(defn myDistinct [table]
  (into [] (distinct table)))

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
    :else {}))


(defn executeSqlQuery [query]
  (def splitedLine (str/split query #" "))
  (def parsedSql (parseSqlQuery splitedLine))
  (def tabl (createTable (get parsedSql :tableName)))
  (def initialTable (if (and (contains? parsedSql :isSelect) (contains? parsedSql :tableName))
                      (selectColumn (getKeys tabl) tabl)
                      (print "error in query")))
  (def tableWithWhere (if(contains? parsedSql :isWhere) (recursiveConcat [] (createArrayOfDfInWhere query tabl) (findAllClausesInWhere (findWhereExpression query)) ) initialTable))
  (def tableWithDistinct (if (contains? parsedSql :isDistinct) (myDistinct tableWithWhere) tableWithWhere))
  (def selectByColumns (if (contains? parsedSql :expressions) (print-formated-hashmap-in-table(selectColumn (get parsedSql :expressions) tableWithDistinct)) (print-formated-hashmap-in-table tableWithDistinct)))
  )

