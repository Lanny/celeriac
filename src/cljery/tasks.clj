(ns celeriac.tasks
  (:require 
    [taoensso.carmine :as car :refer (wcar)]))

(def task-map {
    "rumors.tasks.i_have_add" (fn [arg1 arg2]
        (+ arg1 arg2)) })
