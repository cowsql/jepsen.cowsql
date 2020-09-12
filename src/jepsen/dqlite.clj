(ns jepsen.dqlite
  (:gen-class)
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as jc]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.dqlite [db :as db]
                           [set :as set]
                           [nemesis :as nemesis]]))

(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:set             set/workload})

(defn test
  "Constructs a test from a map of CLI options."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db)
        nemesis       (nemesis/nemesis-package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:primaries :one :majority :majorities-ring]}
                         :pause     {:targets [nil :one :primaries :majority :all]}
                         :kill      {:targets [nil :one :primaries :majority :all]}
                         :interval  (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:name      (str "dqlite-" (name workload-name))
            :pure-generators true
            :os        ubuntu/os
            :db        db
            :checker    (checker/compose
                          {:perf        (checker/perf {:nemeses (:perf nemesis)})
                           :clock       (checker/clock-plot)
                           :stats       (checker/stats)
                           :exceptions  (checker/unhandled-exceptions)
                           :timeline    (timeline/html)
                           :workload    (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                        (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis (:generator nemesis))
                             (gen/time-limit (:time-limit opts)))
                         (gen/log "Healing cluster")
                         (gen/nemesis (:final-generator nemesis))
                         (gen/log "Waiting for recovery")
                         (gen/sleep 2)
                         (gen/clients (:final-generator workload)))})))

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def cli-opts
  "Command line options for tools.cli"
  [["-v" "--version VERSION" "What version of Dqlite should to install"
    :default "master"]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
     :parse-fn parse-nemesis-spec
     :validate [(partial every? #{:pause :kill :partition :clock :member})
                "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 10
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]])


(def single-test-opts
  "CLI options for running a single test"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (jc/one-of workloads))
    :validate [workloads (jc/one-of workloads)]]])


(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (jc/run!
   (merge (jc/serve-cmd)
          (jc/single-test-cmd {:test-fn test
                               :opt-spec (concat cli-opts single-test-opts)}))
   args))
