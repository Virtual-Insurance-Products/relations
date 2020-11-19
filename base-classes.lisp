
(in-package :relations)

;; Is this really needed?
(defclass relation ()
  ((acceptable-string :initarg :acceptable-string :accessor relation-acceptable-string)))


;; a relation which can be nested within things. Is this useful?
(defclass nestable (relation) ()) ; the most general

(defclass selectable (nestable) ())
(defclass projectable (nestable) ())

;; These classes are just used to specify what can be joined...
(defclass join-left-parameter (nestable) ())        ; can only be used on LHS
(defclass join-parameter (join-left-parameter) ())  ; can be used either on left or right of a join

;; some properties of things which can be used to provide specialised operations
;; If we had protocols we could just specify the some operation can be performed on something which provides certain operations
(defclass named () ()) ; !!! MAYBE NOT NEEDED
(defclass explicitly-named (named)
  ((name :reader name :initarg :name)))

(defclass wrapped-relation (relation)
  ((r0 :initarg :r0 :reader r0 :type relation)))

(defclass double-wrapped-relation (wrapped-relation)
  ((r1 :initarg :r1 :reader r1)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; !!! Probably add more things here (see previous definition)
(defclass relation-column (explicitly-named)
  ((table :initarg :table :reader column-table :initform nil)
   (type :initarg :type :reader column-type :initform t)
   (presentation-type :initarg :presentation-type :reader column-presentation-type :initform t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; CONCRETE CLASSES

(defclass limited (wrapped-relation)
  ((count :initarg :count :reader limit-count)
   (r0 :type nestable)))

(defclass ordered (wrapped-relation nestable)
  ((order-by-col :initarg :order-by :reader order-by-col)))

(defclass renamed (explicitly-named wrapped-relation
                                    ;; cannot be selected or projected at the moment.
                                    join-parameter)
  ;; I'll exclude the obvious (rename 'a (rename 'b ...))
  ((r0 :type (and nestable (not renamed)))))

;; A mapping takes a set S and returns another set with the same number of items but transformed somehow
(defclass mapping (relation) 
  ())

;; This can be used as a general mapping - if we transform some other kind of mapping by modifying its relation this is what will result.
(defclass fixed-mapping (mapping wrapped-relation)
  ((mapping-function :initarg :mapping-function :reader mapping-function)
   (relation-columns :initarg :relation-columns :reader relation-columns)
   (attribute-values-function :initarg :attribute-values-function :reader attribute-values-function
                              ;; The following would only really make sense if we get given a list
                              :initform (lambda (x) x))
   ;; the initform defaults this to being the empty type, which has no members. 
   (tuple-type :initarg :tuple-type :reader tuple-type :initform nil)
   (tuple-presentation-type :initarg :tuple-presentation-type :initform nil :reader tuple-presentation-type)))


(defclass projection (nestable wrapped-relation)
  ;; By using relation-columns as the slot we get the columns of a projection 'for free'
  ((relation-columns :initarg :relation-columns :reader relation-columns)
   (r0 :type projectable)
   (distinct :initarg :distinct :initform nil)))


(defclass projection-column (relation-column)
  ((expression :initarg :expression :reader attribute-expression)
   (simple-projection :initarg :simple-projection :initform t :reader simple-projection)))

;; Can this subclass anything else?
(defclass selection (wrapped-relation projectable)
  ((r0 :type selectable) ; Is there a better type I can use for this?
   (expression :initarg :expression :reader expression)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass abstract-join (double-wrapped-relation join-left-parameter projectable selectable)
  ;; I restate these slot definitions purely to add these type constraints
  ((r0 :type join-left-parameter)
   (r1 :type join-parameter)))

;; the following two classes are very similar
(defclass cartesian-product (abstract-join)
  ())

(defclass sql-join (abstract-join)
  ((type :initarg :type :reader join-type)
   (condition :initarg :condition :reader join-condition)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; GROUPING

;; I might need to flesh this out a bit (or possibly redesign it) but
;; I think this should work.

(defclass grouping (projection)
  ((group-expression :initarg :group-expression :reader group-expression)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; The SQL view is used for presenting stuff to send to the database...

(defclass sql-view () ())
(defparameter +sql-view+ (make-instance 'sql-view))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Now let's define a presentation type for relation which is a bit cleverer...

(define-presentation-type relation (&optional (element-type t)))

(define-presentation-method present :around
  ((x relation) (type relation)
   (stream stream) (view textual-view) &key &allow-other-keys)
  (if (slot-boundp x 'acceptable-string)
      (write-sequence (slot-value x 'acceptable-string) stream)
      (call-next-method)))


;; generally for a relation we assume that the tuple type is list...
;; (eg (tuples (pg-table 'brand)))
(defmethod tuple-type ((r relation)) t)

(define-presentation-method presentation-typep (object (type relation))
  (and (typep object 'relations::relation)
       ;; now I have to look at the element type
       (presentation-subtypep (tuple-type object) element-type)
       ;; We can check the following too, and it seems as though we should, but does it help at all?
       ;; (presentation-subtypep (tuple-presentation-type object) element-type)
       ;; Just to suppress multiple values from the above
       t))

;; this is necessarily true:-
(defmethod tuple-type ((r selection)) (tuple-type (r0 r)))

(define-presentation-method presentation-subtypep ((type relation) putative-supertype)
  (with-presentation-type-decoded (super-name super-element-type)
      putative-supertype
    (when (and (eq super-name 'relation)
               (presentation-subtypep element-type (first super-element-type)))
      (values t t))))
