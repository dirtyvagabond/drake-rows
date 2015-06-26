(ns drake.protocol.row
  (:require [rows.core :as rows]
            [clojure.string :as str])
  (:use [drake-interface.core :only [Protocol]]))


;; For use by a row step

(def ^:dynamic <rows>)
(def ^:dynamic <columns> nil)

;; TODO: this presumes exactly one headers for every output
(defn columns [& cs]
  (def <columns> (map name cs)))

(defn tout [output row]
  (with-meta row {:output output}))


;; Manipulate Clojure code into rows

(defn add-ns [clj-str]
  (str "(ns drake.protocol.row)\n" clj-str))

(defn add-rows [clj-str]
  (str "(map (fn [<row>] " clj-str " ) <rows>)" ))

(defn eval-out-rows [clj-str]
  (-> clj-str add-rows add-ns load-string))

(defn do-rows [{:keys [vars cmds outputs inputs]}]
  (def <rows> (rows/merge-in-rows inputs))
  (let [clj-str (str/join "\n" cmds)
        out-rows (eval-out-rows clj-str)]
    (if <columns>
      (rows/write-rows out-rows outputs <columns>)
      (rows/write-rows out-rows outputs))))

(defn row []
  (reify Protocol
    (cmds-required? [_] false)
    (run [_ step]
      (do-rows step))))
