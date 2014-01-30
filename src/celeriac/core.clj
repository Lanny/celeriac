(ns celeriac.core
  (:require 
    [taoensso.carmine :as car :refer (wcar)]
    [clojure.data.json :as json]
    [clojure.data.codec.base64 :as b64])
  (:import
    [java.lang String]
    [java.io FileReader]
    [java.util UUID]
    [java.util.concurrent Executors LinkedBlockingDeque]))

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
(def task-deserializers {
  "application/json" (fn [body encoding]
    (json/read-str
      (String.
        (b64/decode
          (.getBytes body "UTF-8")) encoding) :key-fn keyword))})

(def task-serializers {
  "json" (fn [task]
    (json/write-str {
      :properties {
        :body-encoding "base64"
        :correlation_id (str (UUID/randomUUID))
        :reply_to nil ; Whelp, I have no idea what this is supposed to do
        :delivery_mode 2
        :delivery_tag 1
        :delivery_info {
          :priority 0 ; Oh look, more stuff I'm ignoring
          :routing_key "celery"
          :exchange "celery" }}
      :headers {}
      :content-type "application/json"
      :content-encoding "UTF-8"
      :body (String.
        (b64/encode
          (.getBytes
            (json/write-str task))) "UTF-8") })) })

(def result-serializers {
  "json" json/write-str })

(def result-backends {
  "redis" (fn [task result config]
    (let [{conn :redis-conn serializer-name :result-serializer queue :queue-name} config
        serializer (get result-serializers serializer-name)]
      (car/wcar conn
        (car/set (str queue "-task-meta-" (:id task))
          (serializer result)))))
})

(def broker-consumers 
  "Strategizes the retreival of a task from the broker. All functions here take
  a queue name, timeout (in seconds), and a config and return a task if one is 
  available or nil if one is not within the timeout. The raw task is returned,
  so it'll probably be a string. No effort to deserialize is made."
  {
    "redis" (fn [queue timeout config]
      (last (car/wcar (:redis-conn config) 
        (car/brpop queue timeout))))
  })

(def broker-writers {
  "redis" (fn [task config]
    (let [serializer (get task-serializers (:task-serializer config))
        queue (or (:queue task) "celery")]
      (car/wcar (:redis-conn config)
        (car/rpush queue (serializer task))))) })

(defn apply-async [task-name config &{:keys [args kwargs queue callbacks]
    :or {args [] kwargs {} queue "celery" callbacks nil}}]
  "A convienience function for generating task maps with defaults and sending
  them off to the queue"
  ; totally doesn't work yet
  {:body 
    {:chord nil 
     :id (str (UUID/randomUUID))
     :retries 0
     :args args
     :expires nil
     :timelimit [nil nil]
     :taskset nil
     :task task-name
     :callbacks callbacks
     :kwargs kwargs
     :errbacks nil
     :utc true}
  })

(defn consume-to-queue [internal-queue running config]
  "Polls the broker, per config, continuously as long as running derefs true and
  and internal-queue is not full. Handles decoding the task."
  (let [consume (get broker-consumers (:broker config))]
    (while @running
      ; Even if we have capacity, don't take new tasks if we're shutting down
      (while (and (> (.remainingCapacity internal-queue) 0) @running)
        (let [raw-message (consume (:queue-name config) 
            (:queue-poll-interval config) config)]
          (if raw-message
            ; If timeout is hit raw-message is nil, loop, check if we're
            ; shutting down, and repeat
            (let [message (json/read-str raw-message :key-fn keyword)
              decoder (get task-deserializers (:content-type message))]
            (if-not decoder
              (log :warn "Got task with unexpected content-type:" 
                (:content-type message))
              ; If everythign went well we push into the queue. In theory this
              ; should never block since we checked if we have capasity earlier
              (.offerLast internal-queue {
                :meta message
                :task (decoder
                  (:body message) 
                  (:content-encoding message))}))))))

      ; If we're at capacity but still running then poll internal queue for cap.
      (Thread/sleep 1000)))

  ; Once it's time to shut down we need to return our internally queued tasks 
  ; to the broker
  (let [return (get broker-writers (:broker config))]
    (doall 
      (for [t (iterator-seq (.iterator internal-queue))]
        (return t config))))

  ; And signal any workers waiting on this queue that it's time to pack up
  (doall (for [_ (range (:thread-count config))]
    (.put internal-queue {:terminate true}))))

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

(defn worker-loop [internal-queue running config]
  "Consumes and executes tasks from internal-queue as long as running derefs 
  true."
  (log :debug "Worker loop started")
  (while (deref running)
    (let [{ task :task m-data :meta terminate :terminate } (.takeLast internal-queue)
        transmit-result (get result-backends (:results-backend config))]
      (log :info "Beginning execution of task:" (:task task))
      (log :debug "Full body of that task being:\n" task)

      (try 
        (if terminate nil ; Looks like it's closing time
          (let [result (execute-task task config)]
            (log :info "Completed task " (:id task) " of type " (:task task) 
              " with result: " result)

            (transmit-result task 
              {:status "SUCCESS"
               :traceback nil
               :result result
               :children [] }
              config)

            (if (seq (:callbacks task))
              (doall (for [cb (:callbacks task)]
                (let [broker-writer (get broker-writers (:broker config))]
                  (broker-writer ; If task isn't immutable, add result to args
                    (if (:immutable task)
                      (assoc cb :id (:task_id (:options cb)))
                      (merge cb 
                        {:args (into (:args cb) [result]) 
                         :id (:task_id (:options cb)) })) 
                    config)))))))

        (catch Exception e
          (log :error "Exception occurred wile executing a task. Original "
            "message was:" (.getMessage e))
          (log :info "Stack trace on that was: " 
            (map (fn [x] (str (.toString x) "\n")) (.getStackTrace e)))

          (transmit-result task
            {:status "FAILURE"
             :traceback (map (fn [x] (.toString x)) (.getStackTrace e))
             :result (.getMessage e)
             :children [] }
            config))))))

(def default-config {
  :thread-count (.availableProcessors (Runtime/getRuntime))
  :queue-name "celery"
  :result-serializer "json"
  :queue-poll-interval 1 ; time between polling in seconds
  :redis-conn {
    :pool {}
    :spec { :host "127.0.0.1" :port 6379 }}})

(defn start-worker [config]
  "Initalize a worker (per celery's terminology, a worker is a group of 
  concurrent task executors) with a given config"

  (log :info "Starting worker with the following settings:")
  (log :info "Threads (concurrency):" (:thread-count config))
  (log :info "Registered Tasks:")
  (dorun (for [[task task-fn] (:task-map config)]
    (log :info " - " task)))

  (log :info "\nWorker booted!")

  (let [running (atom true)
      working-config (merge default-config config)
      thread-pool (Executors/newFixedThreadPool 
        (+ (:thread-count config) 1)) ; Add one for the consumer
      internal-queue (LinkedBlockingDeque. (:thread-count working-config))
      concurrent-executors (for [_ (range (:thread-count working-config))]
        (fn [] (worker-loop internal-queue running working-config)))
      consumer (fn [] (consume-to-queue internal-queue running working-config))
      futures (.invokeAll thread-pool (conj concurrent-executors consumer))]
    (doseq [future futures]
      (.get future))))

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
        (start-worker (merge default-config working-settings))))))
