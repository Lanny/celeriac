(ns celeriac.core
  (:require 
    [celeriac.tasks :refer (task-map)]
    [taoensso.carmine :as car :refer (wcar)]
    [clojure.data.json :as json]
    [clojure.data.codec.base64 :as b64])
  (:import
    [java.lang String]))

(def log-level :debug)

(def log-levels {
  :debug 0
  :info 1
  :warn 2
  :error 3
})

(defn log [level & message]
  (if (>= (level log-levels) (log-level log-levels))
    (apply println message)))

(def task-decoders {
  "application/json" (fn [body encoding]
    (json/read-str
      (String.
        (b64/decode
          (.getBytes body "UTF-8")) encoding) :key-fn keyword))})

(def result-serializers
  "json" json/write-str)

(defn get-redis-task [running {conn :redis-conn queue :queue-name interval :queue-poll-interval}]
  "Fetches, deserializes, and returns a task from a redis queue. Will block
  until one is available if the queue is empty"
  (loop []
    (let [raw-message (car/wcar conn (car/lpop queue))]
      (if-not raw-message
        (do 
          (Thread/sleep interval) 
          (if (deref running) (recur) nil))
        (let [message (json/read-str raw-message :key-fn keyword)
            decoder (get task-decoders (:content-type message))]
          (log :info "Got a message from the broker.")
          (log :debug "And the contents of that message are:\n" raw-message)

          (if-not decoder
            (throw (Exception. (str "Task content-type '" decoder "' not supported."))))

          {:meta message
           :task (decoder (:body message) (:content-encoding message)) })))))

(defn fetch-and-execute [running config]
  "Poll for a task until one comes through, execute it, give the result to the
  broker, rinse, and repeat."
  (while (deref running)
    (let [{ task :task m-data :meta } (get-redis-task running config)
          task-fn (get (:task-map config) (:task task)) ; :task is the string task name
          result (apply task-fn (:args task))]
        (log :info "Completed task " (:id task) " of type " (:task task) 
          " with result: " result)

        (car/wcar (:redis-conn config)
          (car/set (str (:queue-name config) "-task-meta-" (:id task))
            (json/write-str
              {:status (if result "SUCCESS" "FAILURE")
               :traceback nil
               :result result
               :children [] }))))))

(def default-config {
  :threads 4
  :queue-name "celery"
  :result-serializer "json"
  :queue-poll-interval 500 ; time between polling in milliseconds
  :redis-conn {
      :pool {}
      :spec { :host "127.0.0.1" :port 6379 }}})

(defn start-worker [config]
  (log :info "Worker booted!")
  (let [running (atom true)
      working-config (merge default-config config)]
    (fetch-and-execute running working-config)))

(defn -main []
  (start-worker {
    :broker "redis"
    :results-bakend "redis"
    :task-map task-map }))
