
(ns drake.protocol.rows
  (:require [rows.core :as rows]
            [clojure.string :as str])
  (:use [drake-interface.core :only [Protocol]]))


;; For use by a rows step

(def ^:dynamic <rows>)
(def ^:dynamic <columns> nil)

;; TODO: this presumes exactly one headers for every output
(defn columns [& cs]
  (def <columns> (map name cs)))

(defn tout [output row]
  (with-meta row {:output output}))


;; Manipulate Clojure code into rows

(defn add-ns [clj-str]
  (str "(ns drake.protocol.rows)\n" clj-str))

(defn eval-out-rows [clj-str]
  (-> clj-str add-ns load-string))

(defn do-rows [{:keys [vars cmds outputs inputs]}]
  (def <rows> (rows/merge-in-rows inputs))
  (let [clj-str (str/join "\n" cmds)
        out-rows (eval-out-rows clj-str)]
    (if <columns>
      (rows/write-rows out-rows outputs <columns>)
      (rows/write-rows out-rows outputs))))

(defn rows
  "Formats the step hash-map and spits it to the step's output file.
   The step hash-map holds all data about the step, and that's what your protocol
   uses to decide what to do. Valuable step data includes:
     :cmds    The step's commands (a.k.a., body), as a sequence, one element per line
     :inputs  All input files specified by the step
     :outputs All output files specified by the step
     :opts    All options specified by the step, as a hash-map"
  []
  (reify Protocol
    (cmds-required? [_] false)
    (run [_ step]
      (do-rows step))))
