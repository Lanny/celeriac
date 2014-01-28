(defproject celeriac "0.1.0-SNAPSHOT"
  :description "A minimal celery worker implementation in clojure."
  :url "https://github.com/RyanJenkins/celeriac"
  :license {:name "GNU Public License v3"
            :url "http://www.gnu.org/licenses/gpl.html"}
  :main ^:skip-aot celeriac.core
  :dependencies [[org.clojure/clojure "1.5.1"]
      [com.taoensso/carmine "2.4.5"]
      [org.clojure/data.json "0.2.4"]
      [org.clojure/data.codec "0.1.0"]
  ])
