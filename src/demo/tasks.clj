(ns demo.tasks
  (:require 
    [clojure.string :as string]))

(def task-map {
  "demo.slow_add" (fn [arg1 arg2]
    (Thread/sleep 4000) 
    (+ arg1 arg2)) 
  
  "demo.add" (fn [arg1 arg2]
    (+ arg1 arg2))

  "demo.make_japaneese" (fn [s]
    (str s ", desu."))

  "demo.make_spanish" (fn [s]
    (string/replace s #"(\w+)" "$1o"))
})
