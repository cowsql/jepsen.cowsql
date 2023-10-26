(ns jepsen.cowsql.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.cowsql [client :as client]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn app-dir
  "Return the working directory where the app will store its data on the node."
  [test node]
  (str (:dir test) "/" (name node)))

(defn app-name
  "Return the name of the application binary inside the app working directory."
  [node]
  (str "app-" (name node)))

(defn app-binary
  "Return the path to the app binary file inside the app working directory."
  [test node]
  (str (app-dir test node) "/" (app-name node)))

(defn app-logfile
  "Return the path to the app log file inside the app working directory."
  [test node]
  (str (app-dir test node) "/app.log"))

(defn app-pidfile
  "Return the path to the app PID file inside the app working directory."
  [test node]
  (str (app-dir test node) "/app.pid"))

(defn app-data
  "Return the path to the cowsql data directory that the app will use."
  [test node]
  (str (app-dir test node) "/data"))

(defn app-core-dump-glob
  "Return the core dump file glob pattern."
  [test node]
  (str (app-data test node) "/core*"))

(defn setup-ppa!
  "Adds the Cowsql PPA to the APT sources"
  [version]
  (let [keyserver "keyserver.ubuntu.com"
        key       "392A47B5A84EACA9B2C43CDA06CD096F50FB3D04"
        line      (str "deb http://ppa.launchpad.net/cowsql/"
                       version "/ubuntu focal main")]
    (debian/add-repo! "cowsql" line keyserver key)))

(defn install!
  "Install the Go cowsql test application."
  [test node]

  ;; If we're not running in local mode, install libcowsql from the PPA.
  (when-not (:local test)
    (info "Installing libcowsql from PPA")
    (c/su
     (setup-ppa! (:version test))
     (debian/install [:libcowsql0])))

  ;; Create the test application working directory.
  (c/exec :mkdir :-p (app-dir test node))

  ;; If we were given a pre-built binary, copy it over, otherwise build it from
  ;; source.
  (if-let [pre-built-binary (:binary test)]
    (c/upload pre-built-binary (app-binary test node))
    (let [source (str (app-dir test node) "/app.go")]
      (info "Building test cowsql application from source")
      (c/su (debian/install [:libcowsql-dev :golang]))
      (c/upload "resources/app.go" source)
      (c/exec "go" "get" "-tags" "libsqlite3" "github.com/cowsql/go-cowsql/app")
      (c/exec "go" "build" "-tags" "libsqlite3" "-o" (app-binary test node) source))))

(defn start!
  "Start the Go cowsql test application"
  [test node]
  (c/exec :mkdir :-p (app-data test node))
  (if (cu/daemon-running? (app-pidfile test node))
    :already-running
    (cu/start-daemon! {:env {:LIBCOWSQL_TRACE "1"
                             :LIBRAFT_TRACE "1"}
                       :logfile (app-logfile test node)
                       :pidfile (app-pidfile test node)
                       :chdir   (app-data test node)}
                      (app-binary test node)
                      :-dir (app-data test node)
                      :-node (name node)
                      :-latency (:latency test)
                      :-cluster (str/join "," (:nodes test)))))

(defn kill!
  "Gracefully kill, `SIGTERM`, the Go cowsql test application."
  [test node]
  (let [signal :SIGTERM]
    (info "Killing" (app-name node) "with" signal "on" node)
    (cu/grepkill! signal (app-name node))
    :killed))

(defn stop!
  "Stop the Go cowsql test application with `stop-daemon!`,
   which will `SIGKILL`."
  [test node]
  (if (not (cu/daemon-running? (app-pidfile test node)))
    :not-running
    (do
      (cu/stop-daemon! (app-pidfile test node))
      :stopped)))

(defn members
  "Fetch the cluster members from a random node (who will ask the leader)."
  [test]
  (client/members test (rand-nth (vec @(:members test)))))

(defn refresh-members!
  "Takes a test and updates the current cluster membership, based on querying
  the test's cluster leader."
  [test]
  (let [members (members test)]
    (info "Current membership is" (pr-str members))
    (reset! (:members test) (set members))))

(defn addable-nodes
  "What nodes could we add to this cluster?"
  [test]
  (remove @(:members test) (:nodes test)))

(defn wipe!
  "Wipes data files on the current node and create a 'removed' flag file to
  indicate that the node has left the cluster and should not automatically
  rejoin it."
  [test node]
  (c/exec :rm :-rf (c/lit (str (app-data test node))))
  (c/exec "mkdir" "-p" (app-data test node))
  (c/exec "touch" (str (app-data test node) "/removed")))

(defn grow!
  "Adds a random node from the test to the cluster, if possible. Refreshes
  membership."
  [test]
  ;; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)

  ;; Can we add a node?
  (if-let [addable-nodes (seq (addable-nodes test))]
    (let [new-node (rand-nth addable-nodes)]
      (info :adding new-node)

      ;; Update the test map to include the new node
      (swap! (:members test) conj new-node)

      ;; Start the new node--it'll add itself to the cluster
      (c/on-nodes test [new-node]
                  (fn [test node]
                    (db/kill! (:db test) test node)
                    (c/exec "mkdir" "-p" (app-data test node))
                    (c/exec "touch" (str (app-data test node) "/rejoin"))
                    (db/start! (:db test) test node)))

      new-node)

    :no-nodes-available-to-add))

(defn shrink!
  "Removes a random node from the cluster, if possible. Refreshes membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)
  ; Next, remove a node.
  (if (< (count @(:members test)) 2)
    :too-few-members-to-shrink

    (let [node (rand-nth (vec @(:members test)))]
      ; Ask cluster to remove it
      (let [contact (-> test :members deref (disj node) vec rand-nth)]
        (info :removing node :via contact)
        (client/remove-member! test contact node))

      ; Kill the node and wipe its data dir; otherwise we'll break the cluster
      ; when it restarts
      (c/on-nodes test [node]
                  (fn [test node]
                    (db/kill! (:db test) test node)
                    (info "Wiping" node)
                    (wipe! test node)))

      ; Record that the node's gone
      (swap! (:members test) disj node)
      node)))

(defn retry
  [retries f & args]
  (let [res (try {:value (apply f args)}
                 (catch Exception e
                   (if (zero? retries)
                     (throw e)
                     {:exception e})))]
    (if (:exception res)
      (recur (dec retries) f args)
      (:value res))))

(defn stable
  [test]
  (retry 5 (fn [] (client/stable test
                                 (rand-nth (vec @(:members test)))))))

(defn health
  [test]
  (retry 5 (fn [] (client/stable test
                                 (rand-nth (vec @(:members test)))
                                 :health))))

(defn primaries
  "Returns the set of all primaries visible to any node in the
  cluster."
  [test]
  (->> (:nodes test)
       (pmap (fn [node]
               (timeout 1000 nil
                        (try
                          (client/leader test node)
                          (catch Exception e
                            ; wooooo
                            nil)))))
       (remove nil?)
       set))

(defn db
  "Cowsql test application. Takes a tmpfs DB which is set up prior to setting
  up this DB."
  [tmpfs]
  (let [primary-cache  (atom [])
        primary-thread (atom nil)]
    (reify db/DB
      (setup! [_ test node]
        "Install and start the test application."
        ;; The tmpfs must exist *before* the application starts
        (when tmpfs
          (db/setup! tmpfs test node))
        (info "Setting up test application")
        (install! test node)
        (start! test node)

        ;; Wait until node is ready
        (retry (:cluster-setup-timeout test) (fn []
                                               (Thread/sleep 1000)
                                               (client/ready test node)))

        ;; Spawn primary monitoring thread. It will periodically refresh the
        ;; cache containing the current list of cluster primaries (i.e. the raft
        ;; leader).
        (c/exec
         (when (compare-and-set! primary-thread nil :mine)
           (compare-and-set! primary-thread :mine
                             (future
                               (let [running? (atom true)]
                                 (while @running?
                                   (try
                                     (Thread/sleep 1000)
                                     (reset! primary-cache (primaries test))
                                     (info "Primary cache now" @primary-cache)
                                     (catch InterruptedException e
                                       (reset! running? false))
                                     (catch Throwable t
                                       (warn t "Primary monitoring thread crashed")))))))))
        )

      (teardown! [_ test node]
        (info "Tearing down test application")
        (when-let [t @primary-thread]
          (future-cancel t))
        (kill! test node)
        (when tmpfs
          (db/teardown! tmpfs test node))
        (Thread/sleep 200) ; avoid race: rm: cannot remove '/opt/cowsql/data': Directory not empty
        (c/exec :rm :-rf (app-dir test node)))

      db/LogFiles
      (log-files [db test node]
        (when (cu/daemon-running? (app-pidfile test node))
          (db/pause! db test node))
        (let [tarball    (str (app-dir test node) "/data.tar.bz2")
              ls-cmd     (str "ls " (app-core-dump-glob test node))
              lines      (-> (try (c/exec "sh" "-c" ls-cmd)
                               (catch Exception e ""))
                              (str/split #"\n"))
              core-dumps (->> lines
                              (remove str/blank?)
                              (into []))
              binary (when (seq core-dumps) (app-binary test node))
              everything (remove nil? [(app-logfile test node) tarball binary])]
          (try
            (c/exec :tar :cjf tarball (app-data test node))
            (catch Exception e (info "caught exception: " (.getMessage e))))
          (when (cu/daemon-running? (app-pidfile test node))
            (db/resume! db test node))
          everything))

      db/Process
      (start! [_db test node]
        (start! test node))

      (kill! [_db test node]
        (kill! test node))

      db/Pause
      (pause!
        [_db _test node]
        (info "Pausing" (app-name node) "on" node)
        (cu/grepkill! :stop (app-name node))
        :paused)

      (resume!
        [_db _test node]
        (info "Resuming" (app-name node) "on" node)
        (cu/grepkill! :cont (app-name node))
        :resumed)

      db/Primary
      (setup-primary! [db test node])
      (primaries [db test]
        @primary-cache))))
