(ns dbms.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.data.csv :as reader]
            [clojure.string :as str]
            [clojure.java.io :as io]))

(defn readFile [name]
  (with-open [reader (io/reader name)]
    (doall
      (reader/read-csv reader))))



(defn formate-data-to-hashmap [heads & info]
  (def headers heads)
  (map #(zipmap headers  %) info))

(defn createTable [name]
  (apply formate-data-to-hashmap (readFile name)))

(defn printData [table]
  (loop [outter 0]                                    ;;Внешний цикл по записям от 0 ... n;
    (when (< outter (count table)) ;; Проверяет количество пройденых записей по hashmap
      (loop [inner 0]                                           ;;Внутренний цикл по каждой колонке n записи
        (when (< inner (count (nth table outter)))                   ;;Проверка на число колонок которое уже пройдено
          (print (format "%30s  ||  " (nth (vals (nth table outter)) inner))) ;;Вывод на экран
          (recur (inc inner))))
      (println "")
      (recur (inc outter))))
  )

(def tab (createTable "mp-assistants.csv"))

(defn printHeaders [headers]
  (loop [itter 0]
    (when (< itter (count headers))                   ;;Проверка на количество пройденых столбиков
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

(defn findWhereExpression [query]
  (def firstInd (+(.indexOf query "where")+6))
  (def lastInd (.indexOf query ";"))
  (subs query firstInd lastInd))

(defn splitWhereClause [whereClause]
  (str/split whereClause #" ")
  )

(defn myBetweenValues [betweenExpr]
  (def firs (+ (int (.indexOf betweenExpr "(")) 1))
  (def secon (int(.indexOf betweenExpr ")")))
  (str/split (subs betweenExpr firs secon) #","))

(defn doBetweens [valuess file col]
  (filterv (fn [line] (and (< (Integer/parseInt(get(select-keys line [col])col)) (nth valuess 1))
                            (> (Integer/parseInt(get(select-keys line [col])col) ) (nth valuess 0))
                            ))
           file))

(defn myDistinct [table]
  (into [] (distinct table)))

  (defn selectColumn [col file]
    (cond
                            (= (first col) "*") (
                                                 (map #(select-keys % (getKeys file)) file)
                                                 )
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

