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
  (def headers (map keyword heads))
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


(defn run
  [& args]
  (println "Введите load(\"название файла\") ")
  (def input-line (read-line))
  (def formated-line (nth(str/split input-line #"\"")1))
  (print-formated-hashmap-in-table (createTable formated-line))
  (run ))
