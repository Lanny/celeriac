(ns demo.tasks
  (:require 
    [taoensso.carmine :as car :refer (wcar)]))

(def task-map {
  "demo.add" (fn [arg1 arg2]
    (+ arg1 arg2)) })
