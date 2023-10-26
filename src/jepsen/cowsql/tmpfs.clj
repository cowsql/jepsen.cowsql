(ns jepsen.cowsql.tmpfs
  "Provides a database and nemesis package which can work together to fill up
  disk space."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nem]
                    [util :as util :refer [meh]]]
            [jepsen.nemesis.combined :as nc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn app-dir
  "Return the working directory where the app will store its data on the node."
  [test node]
  (str (:dir test) "/" (name node)))

(defn app-data
  "Return the path to the cowsql data directory that the app will use."
  [test node]
  (str (app-dir test node) "/data"))

(defrecord DB [size-mb]
  db/DB
  (setup! [this test node]
    (let [dir (app-data test node)
          uid (c/exec :id :-u)
          gid (c/exec :id :-g)]
      (info "Setting up tmpfs at" dir)
      (c/exec :mkdir :-p dir)
      (c/su (c/exec :mount :-t :tmpfs :tmpfs dir
                    :-o (str "uid=" uid ",gid=" gid ",size=" size-mb "M,mode=0755")))))

  (teardown! [this test node]
    (let [dir (app-data test node)]
      (info "Unmounting tmpfs at" dir)
      (c/su (meh (c/exec :umount :-l dir))))))

(def balloon-file
  "The name of the file which we use to eat up all available disk space."
  "jepsen-balloon")

(defn fill!
  "Inflates the balloon file, causing the given DB to run out of disk space."
  [test node db]
  (let [dir (app-data test node)]
    (c/su (try+ (c/exec :dd "if=/dev/zero" (str "of=" dir "/" balloon-file))
                (catch [:type :jepsen.control/nonzero-exit] e
                ; Normal, disk is full!
                  )))
    :filled))

(defn free!
  "Releases the balloon file's data for the given DB."
  [test node db]
  (let [dir (app-data test node)]
    (c/su (c/exec :rm :-f (str dir "/" balloon-file)))
    :freed))

(defrecord Nemesis [db]
  nem/Nemesis
  (setup! [this _test] this)

  (invoke! [_this test op]
    (assoc op :value
           (case (:f op)
             :fill-disk (let [targets (nc/db-nodes test (:db test) (:value op))]
                          (c/on-nodes test targets
                                      (fn [test node] (fill! test node db))))
             :free-disk (c/on-nodes test
                                    (fn [test node] (free! test node db))))))

  (teardown! [_this _test])

  nem/Reflection
  (fs [_this]
    #{:fill-disk :free-disk}))

(defn package
  "Options:
   ```clj
   :faults #{:disk}
   :disk {:targets [nil :one :primaries :majority :all]
          :dir     db/data-dir
          :size-mb 100}
   ```

   Returns:
   ```clj
   {:db              ; tmpfs
    :nemesis         ; disk nemesis for tmpfs
    :generator       ; fill/free disk
    :final-generator ; free disk
    :perf            ; pretty plots            
   ```"
  [{:keys [faults disk interval] :as _opts}]
  (when (:disk faults)
    (let [{:keys [targets size-mb]} disk
          _  (assert (pos? size-mb))
          db (DB. size-mb)
          fills (fn [_ _]
                   {:type  :info
                    :f     :fill-disk
                    :value (rand-nth targets)})
          frees (repeat
                  {:type  :info
                   :f     :free-disk
                   :value :all})
          interval (or interval nc/default-interval)]
      {:db              db
       :nemesis         (Nemesis. db)
       :generator       (->> (gen/flip-flop fills frees)
                             (gen/stagger interval))
       :final-generator (gen/once frees)
       :perf            #{{:name  "disk"
                           :start #{:fill-disk}
                           :stop  #{:free-disk}
                           :color "#99DC58"}}})))
