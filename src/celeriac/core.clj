(ns celeriac.core
  (:require 
    [taoensso.carmine :as car :refer (wcar)]
    [clojure.data.json :as json]
    [clojure.data.codec.base64 :as b64])
  (:import
    [java.lang String]
    [java.io FileReader]))

(def log-level :info)

(def log-levels {
  :debug 0
  :info 1
  :warn 2
  :error 3
})

(defn log [level & message]
  (if (>= (level log-levels) (log-level log-levels))
    (apply println message)))

; LIVE BY STRATEGY DIE BY STRATEGY!!!
(def task-decoders {
  "application/json" (fn [body encoding]
    (json/read-str
      (String.
        (b64/decode
          (.getBytes body "UTF-8")) encoding) :key-fn keyword))})

(def result-serializers {
  "json" json/write-str })

(def result-transmitters {
  "redis" (fn [task result config]
    (let [{conn :redis-conn serializer-name :result-serializer queue :queue-name} config
        serializer (get result-serializers serializer-name)]
      (car/wcar conn
        (car/set (str queue "-task-meta-" (:id task))
          (serializer result)))))
})

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
          (log :debug "And the contents of that message are:\n" message)

          (if-not decoder
            (throw (Exception. (str "Task content-type '" decoder "' not supported."))))

          {:meta message
           :task (decoder (:body message) (:content-encoding message)) })))))

(defn execute-task [task config]
  "Take a task we just pulled off the wire, figure out which, if any, function
  should handle it, and return of value of executing that function"
  (let [task-fn (get (:task-map config) (:task task))
      start-time (System/currentTimeMillis)] 
    (if-not task-fn
      (throw (Exception. (str "Task of unregistered type '" (:task task) 
        "' found in queue.")))
      (let [result (apply task-fn (:args task))
          run-time (float (/ (- (System/currentTimeMillis) start-time) 1000))]
        (log :info "Task '" (:task task) "' finished in " run-time "seconds")
        result))))

(defn fetch-and-execute [running config]
  "Poll for a task until one comes through, execute it, give the result to the
  broker, rinse, and repeat."
  (while (deref running)
    (let [{ task :task m-data :meta } (get-redis-task running config)
        transmit-result (get result-transmitters (:results-backend config))]
      (try 
        (log :debug "Got task, full body is:\n" task)

        (let [result (execute-task task config)]
          (log :info "Completed task " (:id task) " of type " (:task task) 
            " with result: " result)

          (transmit-result task 
            {:status "SUCCESS"
             :traceback nil
             :result result
             :children [] }
            config))

      (catch Exception e
        (log :error "Exception occurred wile executing a task. Original "
          "message was:" (.getMessage e))
        (transmit-result task
          {:status "FAILURE"
           :traceback (.getStackTrace e)
           :result (.getMessage e)
           :children [] }
          config))))))


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

(defn -main 
  ([]
    (println "Please specify a configuration file."))
  ([filename]
    (let [reader (FileReader. filename)
        settings (json/read reader :key-fn keyword)
        [path module task-map] (:task-set settings)]
      (.close reader)

      ; Some hackery to load up the tasks module
      (require [path [module]])
      (let [task-map-symbol (str path "." module "/" task-map)
          working-settings (assoc settings 
            :task-map (eval (read-string task-map-symbol)))]
        (start-worker working-settings)))))
