(ns demo.tasks
  (:require 
    [taoensso.carmine :as car :refer (wcar)]))

(def task-map {
  "demo.slow_add" (fn [arg1 arg2]
    (Thread/sleep 4000) 
    (+ arg1 arg2)) 
  
  "demo.add" (fn [arg1 arg2]
    (+ arg1 arg2)) })
