
(in-package :relations)



(defparameter *relation-members*
  (make-hash-table :test #'eq :weak t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Utility

(defun copy-object (object)
  (let ((new (make-instance (class-of object))))
    (loop for s in (ccl:class-slots (class-of object))
       for name = (ccl:slot-definition-name s)
       when (slot-boundp object name)
       do (setf (slot-value new name) (slot-value object name)))
    new))

(defmethod copy-with-slot-values (object &rest slots)
  (let ((new (copy-object object)))
    (loop for (slot value) on slots by #'cddr
         do (setf (slot-value new slot) value))
    new))

(defmethod copy-with-slot-value (object slot value)
  (apply #'copy-with-slot-values (list object slot value)))

(defun numbered-name (n)
  (intern (format nil "N~A" n) :keyword))

(defun expressionp (object car)
  (and (listp object)
       (eq (car object) car)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; MAPPING OVER TUPLES

;; (general cases)

(defmethod map-tuples-from-innermost-p ((r relation)) nil)
;; If no definition of map-tuples is provided then we will try for one of these...
;; (some things *must* have one of these anyway)
(defmethod map-tuples ((f function) x)
  (map-tuples-of-relation f x x))



(defmethod map-tuples-from-innermost-p ((r wrapped-relation))
  (map-tuples-from-innermost-p (r0 r)))

;; I don't think I can do a meaningful definition yet...
(defmethod map-tuples :around ((f function) (r wrapped-relation))
  (if (map-tuples-from-innermost-p r)
      (map-tuples-of-relation f (r0 r) r)
      (call-next-method)))

(defmethod map-tuples-of-relation ((f function) (r wrapped-relation) (relation-to-map relation))
  (map-tuples-of-relation f (r0 r) relation-to-map))



(defmethod map-tuples ((f function) (r mapping))
  ;; !!! Does the mapping function method need to know the relation?
  ;; Generally I suppose it does really.
  ;; But doesn't that mean we can't set a static function?
  ;; Or should we make a function which returns a function of something
  (let* ((relation (r0 r))
         ;; I'm tempted to just do this...
         (mapping-function (funcall (mapping-function r) relation)))
    (map-tuples (lambda (tuple)
                  (let ((tuple (funcall mapping-function tuple)))
                    ;; This allows us to know that the tuple came from this relation
                    (setf (gethash tuple *relation-members*) r)
                    (funcall f tuple)))
                relation)))



;; We don't want to hand off to the innermost, because we have to use the mapping...
;; Of course, we *will* be defined in terms of mapping those tuples
(defmethod map-tuples-from-innermost-p ((r mapping)) nil) ; for fixed-mapping or mapping ? 



;; ... more to come



(defun tuples (x)
  ;; simple way to get them in the order we map them
  (let* ((result (cons nil nil))
         (this result))
    (map-tuples (lambda (tuple)
                  (setf (cdr this) (cons tuple nil)
                        this (cdr this)))
                x)
    ;; first cell is a dummy one
    (cdr result)))

(defun unique-tuple (x)
  (let ((found nil)
        (result nil))
    (map-tuples (lambda (tuple)
                  (when found
                    (error "Too many rows in ~A!" x))
                  (setf result tuple
                        found t))
                ;; this limit 2 wouldn't be necessary, but if x is a DB relation then it will avoid
                ;; fetching a whole load of data only to ignore it
                (limit 2 x))
    result))

(defun one-tuple (x)
  (unique-tuple (limit 1 x)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; QUERYING THE COLUMNS OF RELATIONS

;; mirroring the :around method of relation-columns above...
;; (I don't *think* I need an alternative to map-tuples-from-innermost-p)
(defmethod relation-columns :around ((r double-wrapped-relation))
  (if (map-tuples-from-innermost-p r)
      (relation-columns-of-relation r r)
      (call-next-method)))

(defmethod relation-columns (r)
  (relation-columns-of-relation r r))

;; in general cases, then, we can figure out columns of different things thusly...
;; !!! This won't always work (see above 2 functions)
(defmethod relation-columns :around ((r renamed))
  (mapcar (lambda (x)
            (copy-with-slot-values x 'table (name r)))
          (relation-columns (r0 r))))

(defmethod relation-columns-of-relation (x (r abstract-join))
  (declare (ignore x))
  (append (relation-columns (r0 r))
          (relation-columns (r1 r))))

(defmethod relation-columns ((r limited))
  (relation-columns (r0 r)))

(defmethod relation-columns ((r ordered))
  (relation-columns (r0 r)))

;; A selection always has the same columns as what it selects over...
(defmethod relation-columns ((r selection))
  (relation-columns (r0 r)))


;; then from all of this we will define the interface function...
(defun columns (r)
  (remove-if (lambda (x)
               (and (listp (name x))
                    (cdr (name x))))
             (relation-columns r)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; TUPLE COUNT

;; General implementation, but we can provide more efficient ones, including on probably
;; just about everything.

;; because we can provide a sensible default I won't just defer to the -of-relation variant
;; this means that the base might have to provide that itself.
(defmethod tuple-count (r)
  (let ((count 0))
    (map-tuples (lambda (x)
                  (declare (ignore x))
                  (incf count))
                r)
    count))

(defmethod tuple-count :around ((r wrapped-relation))
  (if (map-tuples-from-innermost-p r)
      (tuple-count-of-relation r r)
      (call-next-method)))

(defmethod tuple-count-of-relation ((x wrapped-relation) r)
  (tuple-count-of-relation (r0 x) r))

;; I can also provide things like this...
(defmethod tuple-count ((x cartesian-product))
  (* (tuple-count (r0 x))
     (tuple-count (r1 x))))
;; (provided we relax the uniqueness constraint!)

(defmethod tuple-count-of-relation ((x mapping) r)
  (tuple-count-of-relation (r0 x) r))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; ATTRIBUTE VALUES

;; So, given that we have some relation 'r' and we have a tuple of that relation, how do we get the values of the attribuets of that tuple...

;; if the tuples are returned as a list then it is trivial
(defmethod attribute-values-for-relation ((x list) (r t)) x)

(defmethod attribute-values-for-relation (x (r fixed-mapping))
  ;; In this case the fixed mapping has to store this
  (funcall (attribute-values-function r) x))

;; so as to make sure to override the previous one
(defmethod attribute-values-for-relation ((x list) (r fixed-mapping))
  (funcall (attribute-values-function r) x))

;; now let's have a protocol for getting the values of named attributes for a tuple
;; This will be used for projecting mappings


(defmethod attribute-value-for-class (tuple (class standard-object) (attribute symbol))
  (slot-value tuple attribute))

(defmethod attribute-value-for-class (tuple (class standard-object) (attribute list))
  (attribute-value (attribute-value tuple (first attribute))
                   (cdr attribute)))


(defun attribute-value (tuple attribute)
  (if attribute
      (attribute-value-for-class tuple (class-of tuple) attribute)
      ;; the 'nil' attribute is used to terminate recursion and always means 'this'
      tuple))

;; now we can do this:-
;; (attribute-value (find-instance 'broker-brand '(= (@ (broker id)) 4186)) '(brand name))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; ATTRIBUTE REFERENCE TRANSFORMATION

(defun expression-is-attribute-reference (expression)
  (and (consp expression)
       (symbolp (first expression))
       (equal (symbol-name (first expression)) "@")))

;; get the bits of the expression column reference
(defun expression-column-name (expression)
  (or (third expression)
      (second expression)))

(defun expression-table (expression)
  (when (third expression)
    (second expression)))

;; (expression-column-name '(@ id))
;; (expression-column-name '(@ brand id))
;; (expression-table '(@ brand id))
;; (expression-table '(@ id))

;; Is an expression a reference to 
(defmethod expression-references (expression relation-column)
  (and (expression-is-attribute-reference expression)
       (or (equal (expression-column-name expression)
                  (name relation-column))
           ;; !!! This is because path like column names are always lists at the moment
           (equal (list (expression-column-name expression))
                  (name relation-column)))
       (or (not (expression-table expression))
           (eq (expression-table expression)
               (column-table relation-column)))))


;; we can then have a general means to transform an expression given a set of projection columns...
(defun transform-expression-for-columns (expression columns &key only-simple-projection leave-not-found)
  (labels ((walk (expression)
             (cond ((listp expression)
                    (if (expression-is-attribute-reference expression)
                        (let ((found (find expression columns
                                           :test #'expression-references)))
                          (when (and (not found)
                                     (not leave-not-found))
                            (error "No such attribute ~A (attributes are ~A)" expression columns))
                          (when (and only-simple-projection
                                     (not (simple-projection found)))
                            (return-from transform-expression-for-columns nil))
                          (if found
                              (values (attribute-expression found)
                                      (column-presentation-type found))
                              expression))
                        (cons (first expression)
                              (mapcar (lambda (o)
                                        (walk o))
                                      (cdr expression)))))
                   ;; turn a fixed mapping into a simple projection since the mapping function won't effect existence
                   ;; This is ok to do for the case of exists clauses. Not sure about member ones though
                   ((typep expression 'fixed-mapping)
                    (walk (project (relation-columns expression)
                                   (r0 expression))))
                   ((typep expression 'projection)
                    (project (relation-columns expression)
                             (walk (r0 expression))))
                   ((typep expression 'selection)
                    ;; map the nested select expression
                    (select (transform-expression-for-columns (expression expression)
                                                              columns
                                                              :leave-not-found t)
                            (r0 expression)))
                   
                   (t expression))))
    (walk expression)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; SELECTION EXPRESSIONS

;; These are expression we will use to do selects on relations
;; they can be straightforwardly transformed into typespecs so we can check for type membership
;; This probably gives us a very sophisticated presentation type which can be used for checking


(define-presentation-type satisfies-expression (expression tt))

;; now we must define how to transform these expressions into lambdas which we can use to check in CL...
;; (to represent as SQL obviously requires a different transformation)


(defun object-has-slot (object slot-name)
  (find slot-name (ccl:class-slots (class-of object))
        :key #'ccl:slot-definition-name))

;; all this needs to do is use slot value and whatnot
;; object might be a list of objects, one of which might have the requested slot
(defun @ (object path)
    (flet ((f (x)
             (when (object-has-slot x (first path))
               (return-from @ (if (cdr path)
                                  (@ (slot-value x (first path))
                                     (cdr path))
                                  (slot-value x (first path)))))))
      (if (listp object)
          (mapcar #'f object)
          (f object))
      ;; if I get here it wasn't found and I should error
      )

  )

(defun like (string pattern)
  (cl-ppcre:scan (concatenate 'string "^"
                              (cl-ppcre:regex-replace-all "\\\\\\%"
                                                          (cl-ppcre:quote-meta-chars pattern)
                                                          ".*")
                              "$")
                 string))

;; (like "Autoline newry" "Autoline %")
;; (like " Autoline newry" "Autoline %")

(defun transform-expression-to-lambda (x typespec)
  (labels ((map-functor (x)
             (or (second (find x
                               '((= equal)
                                 (like like))
                               :test #'string-equal
                               :key 'first))
                 x))
           (as-list (x)
             (if (listp x) x (list x)))
           (walk (x)
             (if (listp x)
                 (cond ((expression-is-attribute-reference x)
                        (if (third x)
                            `(@ ,(second x) ',(as-list (third x))) ; must recreate the @ symbol to put it into this package
                            `(@ *obj* ',(as-list (second x)))))
                       ;; handle is null etc...
                       (t (cons (map-functor (car x))
                                (mapcar #'walk (cdr x)))))
                 x))
           (destructure-typespec (input spec k)
             (cond ((expressionp spec 'named-type)
                    `(let ((,(second spec) ,input))
                       (declare (ignorable ,(second spec)))
                       ,(funcall k)))
                   ((expressionp spec 'product)
                    (destructure-typespec `(first ,input)
                                          (second spec)
                                          (lambda ()
                                            (destructure-typespec `(second ,input)
                                                                  (third spec)
                                                                  k))))
                   (t (funcall k)))))
    
    `(lambda (*obj*)
       ,(destructure-typespec '*obj*
                              typespec
                              (lambda ()
                                `(let ((*obj* (vip-utils:flatten *obj*)))
                                   ,(walk x)))))))


(define-presentation-method presentation-typep (object (type satisfies-expression))
  (and (presentation-typep object tt)
       (presentation-typep object
                           (transform-expression-to-lambda expression tt))))

;; !!! I might need to provide a type-for-subtype tests here, which might just be t
;; (most likely so I think)


;; These tell us what would satisfy tuples of cartesian products

(define-presentation-type product (&optional (a t) (b t)))

(define-presentation-method presentation-typep (object (type product))
  (and (listp object)
       (presentation-typep (first object) a)
       (presentation-typep (second object) b)
       (not (cddr object))))

;; (presentation-typep '(1 2) '(product number number))
;; (presentation-typep '(1 2) '(product number string))


;; Having done the above, as well as being used to implement the tuple-from-relation type it will be able to be used for
;; implementing a general version of map-tuples for selections



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; LIMIT

;; This is an oddity really, because most of the DB operations can be
;; performed on most of the things, but not on this. This has to occur
;; as the outermost thing

(defmethod limit ((n number) (x limited))
  (if (> n (limit-count x))
      x
      (make-instance 'limited :count n :r0 (r0 x))))

(defmethod limit ((n number) (r mapping))
  (make-instance 'fixed-mapping
                 :tuple-type (tuple-type r)
                 :r0 (limit n (r0 r))
                 :attribute-values-function (lambda (x)
                                              (attribute-values-for-relation x r))
                 :mapping-function (mapping-function r)
                 :relation-columns (relation-columns r)))

;; This is the general case
(defmethod limit ((n number) (r relation))
  (make-instance 'limited :count n :r0 r))

(defmethod order-by ((column string) (r ordered))
  r)

(defmethod order-by ((column string) (r mapping))
  (make-instance 'fixed-mapping
                 :tuple-type (tuple-type r)
                 :r0 (order-by column (r0 r))
                 :attribute-values-function (lambda (x)
                                              (attribute-values-for-relation x r))
                 :mapping-function (mapping-function r)
                 :relation-columns (relation-columns r)))

(defmethod order-by ((column string) (r relation))
  (make-instance 'ordered :order-by column :r0 r))


;; Is this all that is needed?

(defmethod map-tuples ((f function) (r limited))
  (let ((count (limit-count r)))
    (map-tuples (lambda (tuple)
                  (when (<= count 0)
                    (return-from map-tuples nil))
                  (decf count)
                  (funcall f tuple))
                (r0 r))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; PRESENTATION TYPE OF THE TUPLES OF A RELATION

;; Sometimes we can find a presentation type for a relation such that
;; if some object satisfies that presentation type then it is a member
;; of that relation.

;; This gives us a sense of equivalence between a relation and some
;; presentation type

;; However, generally we can't check this for arbitrary relations and
;; tuples, so we must return the empty type.
(defmethod tuple-presentation-type ((x t)) nil)

;; (tuple-presentation-type (pg-table 'broker))
;; -> nil


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; RENAME

(defmethod rename (name (r nestable))
  (make-instance 'renamed :name name :r0 r))

;; a mapping isn't really nestable, but they often *can* be joined, so we need to be able to rename them...
(defmethod rename (name (r mapping))
  (make-instance 'fixed-mapping
                 :tuple-type (tuple-type r)
                 :attribute-values-function (lambda (x)
                                              (attribute-values-for-relation x r))
                 :relation-columns (mapcar (lambda (column)
                                             (etypecase column
                                               ;; Is column always going to be a projection column? It will if we want to do
                                               ;; select &c
                                               (projection-column
                                                (make-instance 'projection-column
                                                               :expression (attribute-expression column)
                                                               :name (name column)
                                                               :table name))))
                                           (relation-columns r))
                 :mapping-function (mapping-function r)
                 :tuple-presentation-type (awhen (tuple-presentation-type r)
                                            (list 'named-type
                                                  name
                                                  (if (expressionp it 'named-type)
                                                      (third it)
                                                      it)))
                 :r0 (r0 r)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; PROJECT

(defmethod projection-column ((x projection-column)) x)

;; !!! Should I add in type &c?
(defmethod projection-column ((x list))
  (make-instance 'projection-column
                 :expression (first x)
                 :name (second x)
                 :presentation-type (or (third x) t)))

;; FLATTEN
;; special case - flatten by mapping column specs
(defmethod project (columns (x projection))
  (make-instance 'projection
                 :relation-columns
                 (mapcar (lambda (column)
                           (multiple-value-bind (expression ptype)
                               (transform-expression-for-columns (attribute-expression column)
                                                                 (relation-columns x))
                             (make-instance 'projection-column
                                            :expression expression
                                            :presentation-type (if (eq t (column-presentation-type column))
                                                                   (or ptype t)
                                                                   (column-presentation-type column))
                                            :name (name column))))
                         (mapcar #'projection-column columns))
                 :r0 (r0 x)))

(defmethod project (columns (x projectable))
  (make-instance 'projection
                 :relation-columns (mapcar #'projection-column columns)
                 :r0 x))

(defmethod project (columns (x mapping))
  (let ((columns (mapcar #'projection-column columns))
        (mapped-columns (relation-columns x)))
    ;; There are 2 ways we can proceed: either transform the mapping into a simple projection if that is permitted
    ;; OR make a projected mapping by transforming the mapping into another mapping
    (flet ((mapping-projection ()
             ;; this is the fallback if we can't de-map 
             (make-instance
              'fixed-mapping
              :r0 (r0 x)
                                          
              ;; !!! These are supposed to be properly wrapped. I think
              :relation-columns
              (mapcar (lambda (column)
                        (multiple-value-bind (expression ptype)
                            (transform-expression-for-columns (attribute-expression column)
                                                              mapped-columns)
                          (make-instance 'projection-column
                                         :name (name column)
                                         :simple-projection nil
                                         :presentation-type (or ptype t)
                                         :expression expression)))
                      ;; !!! TODO? Add back in the 'nested' columns. Is this possible in general? I expect so.
                      columns)
              :mapping-function
              ;; !!! Is it good to flatten the mappings? I think so.
              (let ((f (mapping-function x)))
                (lambda (relation)
                  (let ((f (funcall f relation)))
                    (lambda (tuple)
                      (loop for c in columns
                         for object = (funcall f tuple)
                         for expression = (attribute-expression c)
                         collect (attribute-value
                                  object
                                  (if (expression-is-attribute-reference expression)
                                      (expression-column-name expression)
                                      (error "Don't know how to project ~A" expression))))))))
              ;; NOTE: no useful tuple-presentation-type is possible
              ;; (really? I definitely thought that to be the case. I ought to explain why here)
              )))
      (project
       (mapcar
        (lambda (column)
          (multiple-value-bind (expression ptype)
              (transform-expression-for-columns (attribute-expression column)
                                                mapped-columns
                                                :only-simple-projection t)
            (make-instance
             'projection-column
             :expression (or expression
                             ;; early exit is an easy way to scrap this whole object construction...
                             (return-from project (mapping-projection)))
             :presentation-type (if (eq t (column-presentation-type column))
                                    (or ptype t)
                                    (column-presentation-type column))
             :name (name column))))
        columns)
       (r0 x)))))



;; now, if we project a projection we can simplify things by flattening it out
;; to do this we need a general 


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; SELECT

(defmethod select (expression (r selection))
  (make-instance 'selection
                 :expression `(and ,(expression r)
                                   ,expression)
                 :r0 (r0 r)))

;; the reason for doing this change is so that if we have (select
;; (project (select ...))) we don't get ever more complex objects
(defmethod select (expression (r projection))
  (make-instance 'projection
                 :relation-columns (relation-columns r)
                 :r0 (select (transform-expression-for-columns expression
                                                               (relation-columns r)
                                                               :leave-not-found t)
                             (r0 r))))


;; Should this be this general?
(defmethod select (expression (r selectable))
  (make-instance 'selection :expression expression
                 :r0 r))


;; now, when we do a select on a mapping (including a projection) we have to transform the select expression and then 
;; !!! This assumes the mapping has a way to transform expressions, but that *must* be the case if I'm going to do that
(defmethod select (expression (r mapping))
  (make-instance 'fixed-mapping
                 :tuple-type (tuple-type r) ; tuple type must be same as r
                 :mapping-function (mapping-function r)
                 :relation-columns (relation-columns r)
                 :attribute-values-function (lambda (x)
                                              (attribute-values-for-relation x r))
                 :r0 (select (transform-expression-for-columns expression
                                                               (relation-columns r) :leave-not-found t)
                             (r0 r))
                 :tuple-presentation-type (awhen (tuple-presentation-type r)
                                            (if (expressionp it 'satisfies-expression)
                                                (destructuring-bind (first exp type)
                                                    it
                                                  (declare (ignore first))
                                                  `(satisfies-expression ,(if (expressionp exp 'and)
                                                                              `(and ,@ (cdr exp) ,expression)
                                                                              `(and ,exp ,expression))
                                                                         ,type))
                                                
                                                `(satisfies-expression ,expression
                                                                       ,it)))))




(defmethod group-by (group-expression columns (r projectable))
  (make-instance 'grouping
                 :relation-columns (mapcar #'projection-column columns)
                 :group-expression group-expression
                 :r0 r))

(defmethod group-by (group-expression columns (r projection))
  (group-by (transform-expression-for-columns group-expression
                                              (relation-columns r))
            (mapcar (lambda (column)
                      (make-instance 'projection-column
                                     :expression (transform-expression-for-columns (attribute-expression column)
                                                                                   (relation-columns r))
                                     :name (name column)))
                    (mapcar #'projection-column columns))
            (r0 r)))

;; now to do a grouping on a fixed mapping we can first do a projection and then do a grouping
;; this will work IFF the resulting columns are simple
;; (TBC...)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; JOINS...

;; We no longer need methods here - there is only one set of join classes to create (I think)
;; That might change, but I suspect not.

;; The general case should work on any kind of relation which doesn't
;; give a type error due to the types of the slots
(defmethod product (a b)
  (make-instance 'cartesian-product :r0 a :r1 b))

;; mappings, however, are a different kettle of fish altogether
(defmethod product ((a mapping) (b mapping))
  (let* ((r0-a (r0 a))
         (r0-b (r0 b))
         (a-columns (relation-columns r0-a))
         (b-columns (relation-columns r0-b)))
    (make-instance 'fixed-mapping
                   :mapping-function (let ((fa (funcall (mapping-function a) r0-a))
                                           (fb (funcall (mapping-function b) r0-b)))
                                       (lambda (relation)
                                         (declare (ignore relation))
                                         (lambda (tuple)
                                           ;; We will compose the tuple as (list LH RH)
                                           ;; I'm going to need to split the tuple according to the number of columns
                                           (list (funcall fa (subseq tuple 0 (length a-columns)))
                                                 (funcall fb (subseq tuple (length a-columns)))))))
                   :relation-columns (loop for col in (append (relation-columns a)
                                                              (relation-columns b))
                                        for i from 0
                                        collect (copy-with-slot-values col 'expression
                                                                      (list '@ (numbered-name i))))
                   :r0 (project (let ((i 0))
                                  (apply #'append
                                         (loop for cols in (list a-columns
                                                                 b-columns)
                                            for table in '(l r)
                                            collect
                                              (loop for col in cols
                                                 collect
                                                   (copy-with-slot-values col
                                                                          'name (numbered-name i)
                                                                          'expression `(@ ,table ,(name col)))
                                                 do (incf i))))) 
                                (product (rename 'l r0-a)
                                         (rename 'r r0-b)))
                   :attribute-values-function (lambda (x)
                                                (append (attribute-values-for-relation (first x) a)
                                                        (attribute-values-for-relation (second x) b)))
                   :tuple-presentation-type `(product ,(tuple-presentation-type a)
                                                      ,(tuple-presentation-type b)))))

;; QUESTION: Can I define a product of a mapping and not a mapping and vice versa? Probably. In both cases we will make a mapping
;; Don't see why not


(defmethod join (type condition a b)
  (make-instance 'sql-join :type type :condition condition
                 :r0 a :r1 b))

(defun inner-join (condition a b)
  (join :inner condition a b))
(defun left-join (condition a b)
  (join :left condition a b))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; PRINTING...

;; !!! This is not using presentation types, which it should (because that would be better)
;; !!! - especially when we can get objects directly as tuple attributes
;; !!! I need to get the count back like in the other ones.
(defmethod print-object ((x relation) (s stream))
  (let ((*print-circle* nil))
    (print-unreadable-object (x s)
      (format s "~A" (type-of x))
      
      ;; now I need to do the nice tabulation thing. I might print it like an org table
      ;; first I need to ascertain what columns it contains...
      (when *print-pretty*
        (format s "~%")
        (flet ((repeated (n &optional (what " "))
                 (loop for i from 1 to n do (format s "~A" what))))

          (let* ((columns (columns x))
                 ;; Maybe I shouldn't do this automatically here
                 (tuples (tuples (limit 10 x)))
                 (widths (make-array (length columns) :initial-element 0))
                 (all-same-table (every (lambda (x)
                                          (equal (column-table (first columns))
                                                 (column-table x)))
                                        (cdr columns))))
            ;; now loop over each tuple generating the right sort of string for it...
            (when (and (column-table (first columns))
                       all-same-table)
              (format s "~A~%" (column-table (first columns)))
              (repeated (length (format nil "~A" (column-table (first columns))))
                        "=")
              (format s "~%~%"))
      
            (let ((data (cons
                         ;; header row
                         (loop for column in columns
                            for i from 0
                            for string = (format nil "~A~A"
                                                 (if (and (column-table column)
                                                          (not all-same-table))
                                                     (format nil "~A." (column-table column)) "")
                                                 (name column))
                            do (setf (elt widths i) (length string))
                            collect string)
       
                         (loop for tuple in tuples
                            collect (loop for value in (attribute-values-for-relation tuple x)
                                       for column in columns ; for the type...
                                       for col-index from 0
                                       for string = (if (eq value :null)
                                                        "<NULL>"
                                                        (present-to-string value (column-presentation-type column))) 
                                       when (> (length string) (elt widths col-index))
                                       do (progn
                                            column ; ignore
                                            (setf (elt widths col-index) (length string)))
                                       collect string)))))
              (loop for row in data
                 for row-index from 0
                 when (= row-index 1)
                 do (format s "|-")
                   (loop for width across widths
                      for i from 1
                      do 
                        (repeated width "-")
                        (if (= i (length widths))
                            (format s "-|~%")
                            (format s "-+-")))
                 do
                   (format s "| ")
                   (loop for column in row
                      for width across widths
                      do (format s "~A " column)
                        (repeated (- width (length column)))
                        (format s "| "))
               
                   (format s "~%")))))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; MODIFICATION...

(defun column-value ()
  (error "not implemented"))


(defun (setf column-value) (new object column)
  (setf (column-value-of-relation object object column) new))

(defmethod (setf column-value-of-relation) (new (object selection) outer column)
  (setf (column-value-of-relation (r0 object) outer column) new))



(defmethod delete-tuples-of-relation ((object selection) outer)
  (delete-tuples-of-relation (r0 object) outer))

(defun delete-tuples (r)
  (delete-tuples-of-relation r r))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; A type representing some relation and a tuple from that relation

(defun expression-contains-nested-relation (expression)
  (labels ((f (x)
             (when (consp x)
               (f (car x))
               (f (cdr x)))
             (when (typep x 'rel:relation)
               (return-from expression-contains-nested-relation
                 t))
             nil))
    (f expression)))

(define-presentation-type tuple-from-relation (&optional (relation-type t)))

;; !!! I need to handle failure to make a lambda expression by retrieving the tuples and making sure it is one of them
;; that will be expensive, but (I suppose) necessary
;; It will only be necessary when we execute a command which uses such a thing, where it is a legitimate thing to do.
;; ACTUALLY - this is a bit of a problem, because we will keep getting the relation over and over when we check for it being
;; in the command, unless we present tuples (rows) as being tuples of a relation
;; The table view (or similar) might need an option to do that I suppose.
;; I'm not 100% sure that this all really helps us. Depends how the presentation translator is implemented.
(define-presentation-method presentation-typep (object (type tuple-from-relation))
  (and (listp object)
       (= (length object) 2)
       (destructuring-bind (relation tuple)
           object
         ;; We can shortcircuit this test
         (or (eq (gethash tuple *relation-members*) relation)
             (let ((rel (gethash tuple *relation-members*)))
               (and rel
                    (slot-boundp rel 'acceptable-string)
                    (slot-boundp relation 'acceptable-string)
                    (equal (relation-acceptable-string rel)
                           (relation-acceptable-string relation))))
             ;; (gethash tuple *relation)
             (and (presentation-typep relation `(relation ,relation-type))
                  (let ((tuple-presentation-type (tuple-presentation-type relation)))
                    (if (expression-contains-nested-relation tuple-presentation-type)
                        (aif (let ((class (find-class relation-type)))
                               (and (typep class 'postgres-class)
                                    (simple-primary-key class)))
                             (when (tuples (select `(= (@ ,it)
                                                       ,(primary-key-value tuple))
                                                   relation))
                               t)
                             (error "Unable to resolve tuple-presentation-type.")) 
                        (presentation-typep tuple tuple-presentation-type))))))))

(define-presentation-method presentation-subtypep ((type tuple-from-relation) putative-supertype)
  (with-presentation-type-decoded (name parameters)
      putative-supertype
    name
    ;; what determines this?
    (when (presentation-subtypep relation-type (first parameters))
      (values t t))))

(define-parser-acceptor tuple-from-relation
  (simple-parser:sp-try
   (lambda ()
     (awhen (accept `(relation ,relation-type) :stream stream)
       ;; This is needed to be sure that we're trying to accept a tuple from relation. This necessitates the sp-try
       (when (simple-parser:sp-scan "^\\s*->\\s*")
         ;; now we need to accept an object which is an instance of said relation...
         (list it
               (let ((tuple-type (if (eq relation-type t)
                                     (tuple-type it)
                                     relation-type)))
                 (or (accept tuple-type :stream stream)
                     (simple-parser::sp-error "Expected ~A" tuple-type)))))))))



;; Should I use something other than a space here? Maybe a slash? comma?
;; It might be handy to make it obvious that it's not just separate parameters
;; plus it would help to make it clear that this definitely is what we mean
(define-presentation-method present ((x list) (type tuple-from-relation)
                                     (stream stream) (view textual-view) &key &allow-other-keys)
  (format stream "~A -> ~A"
          (present-to-string (first x) `(relation ,relation-type))
          (present-to-string (second x) (if (eq relation-type t)
                                            (tuple-type (first x))
                                            relation-type))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; SLOT VALUE CHANGE SPECIFICATION

;; Building on the above we can make a presentation type for all the information needed to establish a new value for a slot of some instance
(define-presentation-type new-slot-value-specification (&optional (relation t) (slots '*)))

;; (fdefinition '(setf relations:name))

;; return a list of (slot writer-function) pairs for all writeable slots
(defun slot-writers (instance)
  (loop for slot-definition in (ccl:class-slots (class-of instance))
     for slot-name = (ccl:slot-definition-name slot-definition)
     for direct-slot-definition = (find slot-name
                                        (ccl:class-direct-slots (ccl::slot-definition-class slot-definition))
                                        :key #'ccl:slot-definition-name)
     for writer-function = (first (ccl:slot-definition-writers direct-slot-definition))
     when writer-function
     collect (list slot-name (fdefinition writer-function)
                   (ccl:slot-definition-type slot-definition)
                   (aif (typep slot-definition 'postgres-class-slot-definition)
                        (slot-definition-presentation-type slot-definition)
                        t))))

;; (slot-writers (find-instance 'v1:broker 'id 4186))

(define-presentation-method presentation-typep (object (type new-slot-value-specification))
  (and (listp object)
       (= (length object) 4)
       (destructuring-bind (example-relation instance slot-name value)
           object
         (and (presentation-typep (list example-relation instance)
                                  `(tuple-from-relation ,relation))
              (or (eq slots '*) (member slot-name slots))
              (let ((slot (find slot-name (slot-writers instance)
                                :key #'first)))
                (when slot
                  (destructuring-bind (name writer type ptype)
                      slot
                    (declare (ignore writer name))
                    (and (typep value type)
                         (presentation-typep value ptype)))))
              
              t))))

(define-parser-acceptor new-slot-value-specification
  (awhen (accept `(tuple-from-relation ,relation) :stream stream)
    (simple-parser:sp-skip-whitespace)
    ;; now identify the slot
    (let ((instance (second it)))
      (or (loop for (name nil nil ptype) in (slot-writers instance)
             when (and (or (eq slots '*)
                           (member name slots))
                       (simple-parser:sp-scan (format nil "^~A\\s*=\\s*"
                                                      name)
                                              :ignore-case t))
             return (append it
                            (list name
                                  (or (accept ptype :stream stream :delimiter-gestures (list #\Space))
                                      (simple-parser:sp-error "Expected ~A" ptype)))))
          (simple-parser:sp-error "Expected <slot> = <value>")))))


(define-presentation-method present ((x list) (type new-slot-value-specification)
                                     (stream stream) (view textual-view) &key &allow-other-keys)
  (destructuring-bind (rel tuple slot value)
      x
    (let* ((writer (find slot (slot-writers tuple) :key #'first))
           (ptype (fourth writer)))
      (format stream "~A ~A = ~A"
              (present-to-string (list rel tuple) `(tuple-from-relation ,relation))
              (string-downcase (format nil "~A" slot))
              (present-to-string value ptype :acceptably t)))))


(defun relation-from-command (relation command &optional (command-table clim-internals::*active-command-table*))
  (let ((clim-internals::*active-command-table* command-table))
    (setf (relation-acceptable-string relation)
          (present-to-string command 'command :view (make-instance 'textual-view)))
    relation))



;; note that the name and original name must be different, but they can just have different packages
(defmacro wrap-command (name original-name destination-table source-table &rest options)
  (let* ((original (find-command original-name (symbol-value source-table)))
         (parameters (clim-internals::command-parameters original)))
    `(define-command (,name :command-table ,destination-table
                            :options ',(append options
                                               (clim-internals::command-options original))
                            :name ,(command-name original)
                            :return-type ',(command-return-type original))
         ,(loop for (name type) in parameters
             collect (list name
                           (labels ((opts (type opts)
                                      (if opts
                                          (cons type opts)
                                          type))
                                    (map-type (type)
                                      (with-presentation-type-decoded (name parameters options)
                                          type
                                        (cond ((typep (find-class name nil) 'postgres-class)
                                               (opts `(tuple-from-relation ,name) options))
                                              ((member name '(and or))
                                               (opts (cons name (mapcar #'map-type parameters))
                                                     options))
                                              (t type)))))
                             (map-type type))))

       (let ,(labels ((unwrap (type)
                        (with-presentation-type-decoded (name parameters)
                            type
                          (or (typep (find-class name nil) 'postgres-class)
                              (and (member name '(and or))
                                   (unwrap (first parameters)))))))
               

               (loop for (name type) in parameters
                    when (unwrap type)
                  collect (list name `(second ,name))))
         (,(command-symbol-name original) ,@ (mapcar #'first parameters))))))
