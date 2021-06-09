
(in-package :relations)

;; This is where it gets interesting - we make CLOS classes which are also relations stored in a Postgres database

(defparameter *suppress-database-insertion* nil)

;; table, type, name, expression + ...
(defclass slot-projection-column (projection-column)
  ((target-object :initarg :target-object :reader attribute-target-object)
   (target-slot :initarg :target-slot :reader attribute-target-slot)
   (target-class :initarg :target-class :reader attribute-target-class)
   (foreign-key-ref :initarg :foreign-key-ref :reader attribute-foreign-key-ref
                    :initform nil)
   (path :initarg :path :reader attribute-path)
   (primary-key-p :initarg :primary-key-p :initform nil :reader primary-key-p)))

(defclass superclass-projection-column (projection-column)
  ((target-object :initarg :target-object :reader attribute-target-object)
   (target-class :initarg :target-class :reader attribute-target-class)))

(defmethod print-object ((x slot-projection-column) (s stream))
  (print-unreadable-object (x s)
    (format s "~A (~A.~A)"
            (class-name (class-of x))
            (attribute-target-class x)
            (attribute-target-slot x))))


(defclass postgres-class-slot-definition (ccl:standard-slot-definition)
  ((presentation-type :initarg :presentation-type :reader slot-definition-presentation-type)
   (database-generated :initarg :database-generated :reader slot-definition-database-generated
                       :initform nil)))

(defmethod slot-definition-presentation-type :around ((x postgres-class-slot-definition))
  (if (slot-boundp x 'presentation-type)
      (call-next-method)
      ;; if it's not bound then just use the slot's type instead. This assumes it makes for a valid presentation type
      (ccl:slot-definition-type x)))

(defclass postgres-class-direct-slot-definition (ccl:standard-direct-slot-definition postgres-class-slot-definition)
  ())

(defclass postgres-class-effective-slot-definition (ccl:standard-effective-slot-definition postgres-class-slot-definition)
  ())

;; This represents (in abstract) a relation whose tuple attributes can be CLOS instances and which generates an SQL query
(defclass postgres-class (mapping standard-class)
  ((primary-key :initarg :primary-key :reader primary-key)
   (source-table :initarg :source-table :reader source-table)
   (source-relation :initarg :source-relation :accessor source-relation)))

(defun check-foreign-key-declaration (class)
  (when (and (slot-boundp class 'primary-key)
             ;; I don't know what to do about non-simple primary keys here
             (not (second (slot-value class 'primary-key))))
    (unless (find (first (slot-value class 'primary-key))
                  (ccl:class-direct-slots class)
                  :key #'ccl:slot-definition-name)
      (error "The declared primary key (~A) must be a direct slot of the class"
             (slot-value class 'primary-key)))))

;; this will clear it out if we reinitialize
(defmethod reinitialize-instance :before ((x postgres-class) &rest initargs)
  (declare (ignore initargs))
  (slot-makunbound x 'primary-key))

(defmethod reinitialize-instance :after ((x postgres-class) &rest initargs)
  (declare (ignore initargs))
  (check-foreign-key-declaration x))

(defmethod initialize-instance :after ((x postgres-class) &rest initargs)
  (declare (ignore initargs))
  (check-foreign-key-declaration x))

(defmethod source-table :before ((x postgres-class))
  (unless (slot-boundp x 'source-table)
    (setf (slot-value x 'source-table)
          (class-name x))))

(defmethod source-relation :before ((x postgres-class))
  (unless (slot-boundp x 'source-relation)
    (setf (slot-value x 'source-relation)
          (pg-table (source-table x)))))

(defmethod ccl:direct-slot-definition-class ((class postgres-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'postgres-class-direct-slot-definition))

(defmethod ccl:effective-slot-definition-class ((class postgres-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'postgres-class-effective-slot-definition))

(defmethod ccl:compute-effective-slot-definition :around ((class postgres-class) name direct-slots)
  (declare (ignore name))
  (let ((def (call-next-method))
        (presentation-types (loop for slot in direct-slots
                                 when (slot-boundp slot 'presentation-type)
                                 collect (slot-value slot 'presentation-type))))
    (when presentation-types
      (setf (slot-value def 'presentation-type)
            (if (find (first presentation-types)
                      (cdr presentation-types)
                      :test-not #'equal)
                `(and ,@ (mapcar #'slot-definition-presentation-type direct-slots))
                (first presentation-types))))
    def))

(defmethod ccl:validate-superclass ((class postgres-class) (superclass standard-class)) t)

;; This one allows creation of 'programmatic classes' a'la AMOP
(defmethod ccl:validate-superclass ((superclass standard-class) (class postgres-class)) t)




(defun dynamically-generated-class-p (class)
  (and (typep class 'standard-class)
       (not (symbolp (class-name class)))))


(defmethod print-object ((x postgres-class) (s stream))
  (if (dynamically-generated-class-p x)
      (print-unreadable-object (x s)
        (format s "~A" (mapcar #'class-name (ccl:class-direct-superclasses x))))
      (if *print-pretty*
          (call-next-method)
          (print-unreadable-object (x s)
            (format s "~A ~A" 'postgres-class (class-name x))))))

;; now, we have to provide a wrapper around expresion-to-sql to deal with this...



(defmethod relation-columns ((c postgres-class))
  (mapcar (lambda (column)
            (make-instance 'projection-column
                           :expression (list '@ (name column))
                           :type (column-type column)
                           :presentation-type (column-presentation-type column)
                           :table (class-name c)
                           :simple-projection (simple-projection column)
                           :name (attribute-path column)))
          ;; Remove columns form subclasses but leave in columns from FK linked classes
          (let ((classes (mapcar #'class-name (ccl:class-precedence-list c))))
            (remove-if-not (lambda (column)
                             ;; !!! Leave in the FK references but take out any subclass columns
                             (and (typep column 'slot-projection-column)
                                  (or (cdr (attribute-path column))
                                      (member (attribute-target-class column)
                                              classes))))
                           (relation-columns (r0 c))))))

(defun extract-attribute (attribute &optional extend-path)
  (let ((new (make-instance (class-of attribute))))
    (loop for slot in (ccl:class-slots (class-of attribute))
       for name = (ccl:slot-definition-name slot)
       do
         (setf (slot-value new name)
               (cond ((eq name 'expression)
                      `(@ ,(name attribute)))
                     ((and extend-path
                           (eq name 'path))
                      (cons extend-path (attribute-path attribute)))
                     ;; on doing extraction we never take the primary key from what we're extracting
                     ((eq name 'primary-key) nil)
                     (t (slot-value attribute name)))))
    new))

;; This will only work on projects of things which expose a primary key
(defmethod primary-key ((x projection))
  ;; This will error if we don't find one - only call this if you expect and require one
  (name (find-if #'primary-key-p
                 (relation-columns x))))

;; What should we return when none is defined?
(defmethod primary-key :around ((c postgres-class))
  (if (slot-boundp c 'primary-key)
      (call-next-method)
      ;; otherwise see if any of my superclasses have one
      (or (loop for class in (ccl:class-direct-superclasses c)
             for pk = (and (typep class 'postgres-class)
                           (primary-key class))
             when pk return pk)
          (call-next-method))))

;; This is a very blunt mechanism. I could easily enhance it to make it more fine grained.
(defparameter *no-fk-joins* nil)
(defparameter *unjoined-classes* nil)
(defmacro without-fk-references (&body whatever)
  `(let ((*no-fk-joins* t))
     ,@whatever))

(defmacro without-joining (classes &body whatever)
  `(let ((*unjoined-classes* ',classes))
     ,@whatever))

(defgeneric r0-join-condition (from to slot-name primary-key))

(defmethod r0-join-condition ((from postgres-class) to slot-name primary-key)
  (let ((table-name (source-table from)))
    `(= (@ ,table-name ,slot-name)
        (@ ,primary-key))))

(defmethod r0 ((class postgres-class))
  (unless (dynamically-generated-class-p class) ; what to do if it is?
    (labels ((visit (class object-id visited
                           &optional superclass-column-name superclass)
               (let* ((table-name (source-table class))
                      (rel (source-relation class))
                      (attributes nil))
                 ;; if a table references another instance of the same
                 ;; table (or anything in the same class DAG) then it
                 ;; will all go really wrong. Don't do that.

                 (when superclass-column-name
                   ;; This won't appear as the value of any object's slot
                   (push (make-instance 'superclass-projection-column
                                        ;; !!! This assumes that the name of the column referencing the
                                        ;; superclass will always by the name of the table of the superclass.
                                        ;; I should make a way of overriding this assumption. 
                                        :expression `(@ ,table-name ,(source-table superclass))
                                        :name superclass-column-name
                                        :target-class (class-name class)
                                        :target-object object-id)
                         attributes))

                 (let ((visited (cons class visited)))
                   (flet ((link-other-classes (classes &key super-p)
                            (loop for other in classes
                               when (and (typep other 'postgres-class)
                                         (not (dynamically-generated-class-p other))
                                         (not (member other visited)))
                               do
                                 (let* ((superclass-column-name (unless super-p
                                                                  (gensym "SUB")))
                                        (other-relation (visit other object-id visited
                                                               superclass-column-name
                                                               class)))
                                   (setf rel
                                         (if super-p
                                             (let ((merged (join :inner
                                                                 `(= (@ ,table-name ,(source-table other))
                                                                     (@ ,(name (find (simple-primary-key other)
                                                                                     (relation-columns other-relation)
                                                                                     :key #'attribute-target-slot))))
                                                                 rel (rename (gensym "ST") other-relation))))

                                               (push (make-instance 'superclass-projection-column
                                                                    ;; !!! This assumes that the name of the column referencing the
                                                                    ;; superclass will always by the name of the table of the superclass.
                                                                    ;; I should make a way of overriding this assumption. 
                                                                    :expression `(@ ,table-name ,(source-table other))
                                                                    :name superclass-column-name
                                                                    :target-class (class-name class)
                                                                    :target-object object-id)
                                                     attributes)
                                               merged)
                                             (join :left
                                                   `(= (@ ,superclass-column-name)
                                                       ,(if (member (simple-primary-key class)
                                                                    (ccl:class-direct-slots class)
                                                                    :key #'ccl:slot-definition-name)
                                                            ;; If the PK is a direct slot then the following will work
                                                            `(@ ,table-name ,(simple-primary-key class))
                                                            ;; otherwise we find which superclass the PK comes from and then
                                                            ;; use the FK superclass reference column to match...
                                                            `(@ ,table-name
                                                                ;; This loop could be redone as something else, but this is clear
                                                                ,(or (loop for super in (ccl:class-direct-superclasses class)
                                                                        when (member (simple-primary-key class)
                                                                                     (ccl:class-slots super)
                                                                                     :key #'ccl:slot-definition-name)
                                                                        return (source-table super))
                                                                     (error "Cannot find class which defines primary key ~A"
                                                                            (simple-primary-key class))))))
                                                   
                                                     
                                                   rel (rename (gensym "SB") other-relation))))
                                 
                                   ;; now we have to re-project all the attributes
                                   ;; visit will always return a projection
                                   (mapcar (lambda (a)
                                             (push (extract-attribute a) attributes))
                                           (relation-columns other-relation))))))

                     (link-other-classes (ccl:class-direct-superclasses class) :super-p t)
                     ;; !!! Exclude dymamically generated ones
                     (link-other-classes (ccl:class-direct-subclasses class) :super-p nil)))

                 ;; map over the direct slots of this class
                 ;; what shall I do with them?
                 ;; I need to note the pk, because that's what I will join on
                 (loop for slot in (ccl:class-direct-slots class)
                    for slot-type-class = (when (symbolp (ccl:slot-definition-type slot))
                                            (find-class (ccl:slot-definition-type slot) nil))
                    for fk = (typep slot-type-class 'postgres-class)
                    for slot-name = (ccl:slot-definition-name slot)
                    for pk = (and (slot-boundp class 'primary-key)
                                  (equal (list slot-name)
                                         (primary-key class)))
                    do
                      (let ((fk-id (when fk (gensym "I"))))
                        (when (and fk (not *no-fk-joins*)
                                   slot-type-class
                                   (not (member (class-name slot-type-class) *unjoined-classes*)))
                          (let* ((fk-relation (visit slot-type-class fk-id nil))
                                 (primary-key (name (or (find-if #'primary-key-p
                                                                 (remove fk-id (relation-columns fk-relation)
                                                                         :key #'attribute-target-object
                                                                         :test-not #'eq))
                                                        (error "Class ~A has no primary key so can't be joined as FK"
                                                               slot-type-class)))))
                            (setf rel
                                  (join :left (r0-join-condition class fk-relation slot-name primary-key)
                                        rel (rename (gensym "FK") ; rename so that arbitrary joins will work
                                                    fk-relation)))
                          
                            ;; re-project and wrap
                            (mapcar (lambda (a)
                                      (when (typep a 'slot-projection-column)
                                        (push (extract-attribute a slot-name) attributes)))
                                    (relation-columns fk-relation))))
                      
                        ;; in any case, we need to include the slot's column in the projection
                        (push (make-instance 'slot-projection-column
                                             :expression `(@ ,table-name ,slot-name)
                                             :name (gensym "C")
                                             :target-object object-id
                                             :target-slot slot-name
                                             :target-class (class-name class)
                                             :type (ccl:slot-definition-type slot)
                                             :presentation-type (slot-definition-presentation-type slot)
                                             :path (list slot-name)
                                             :foreign-key-ref (and (not *no-fk-joins*)
                                                                   slot-type-class
                                                                   (not (member (class-name slot-type-class) *unjoined-classes*))
                                                                   fk-id)
                                             ;; This is a simple projection if it is not an FK
                                             ;; this allows project to turn a mapping into just a projection without the need
                                             ;; to wrap the mapping stuff
                                             :simple-projection (not fk-id)
                                             :primary-key-p pk)
                              attributes)))
               
                 
                 
                 ;; return the final relation
                 (project (reverse attributes) rel))))

      (let ((r (visit class :root nil)))
        ;; !!! Rename all the output columns so they don't change on each call to this
        (loop for i from 0
           for c in (relation-columns r)
           do (setf (slot-value c 'name) (numbered-name i)))
        r))))


(defmethod mapping-function ((x postgres-class))
  (lambda (relation)
    (let ((attributes (relation-columns relation)))
      (lambda (row)
        ;; now we have to construct the object graph
        ;; We should be able to just iterate over the slots of the projection in order
                  
        (let ((graph (make-hash-table))
              ;; we should use the attribute-values protocol, even though it's not necessary with pg relations
              (row (attribute-values-for-relation row x)))

          (let ((*suppress-database-insertion* t))
            (loop for value in row
               for att in attributes
               when value               ; if it's not nil
               do (push (attribute-target-class att)
                        (gethash (attribute-target-object att) graph)))

            (let ((keys (loop for k being the hash-keys of graph collect k)))
              (loop for k in keys
                 for unique = (remove-duplicates (gethash k graph))
                 for v = (remove-if (lambda (this)
                                      (find-if (lambda (sub)
                                                 (and (not (eq this sub))
                                                      (subtypep sub this)))
                                               unique))
                                    unique)
                 do
                   (setf (gethash k graph)
                         (if (cdr v)
                             ;; ***LOCKING***
                             (vip-utils:allocate-programmatic-instance v)
                             ;; better to call this than make-instance
                             ;; - we aren't creating a new object, just thawing one from the database.
                             ;; We don't want initialize-instance methods to fire. 
                             (allocate-instance (find-class (first v)))))))
                    
                    
            (loop for value in row
                  for att in attributes
                  ;; !!! Ideally I want to be able to distinguish null
                  ;; should I check the class has the slot?
                  when (typep att 'slot-projection-column)
                  do
                     (with-slots (target-object target-slot foreign-key-ref target-class)
                         att
                       (let ((object (gethash target-object graph)))
                         (if value
                             (setf (slot-value object target-slot)
                                   (if foreign-key-ref
                                       ;; we have to look up the object in the graph...
                                       (gethash foreign-key-ref graph)
                                       value))
                             (when (and (typep object target-class)
                                        (typep nil (ccl:slot-definition-type
                                                    (find target-slot
                                                          (ccl:class-slots (find-class target-class))
                                                          :key #'ccl:slot-definition-name))))
                               (setf (slot-value object target-slot) nil)))))))
          
          (gethash :root graph))))))


;; the attributes we report will be only those for the given class. If it's a subclass we still only get those ones.
(defmethod attribute-values-for-relation (x (class postgres-class))
  (loop for slot in (mapcar #'ccl:slot-definition-name (ccl:class-slots class))
     collect (if (slot-boundp x slot)
                 (slot-value x slot)
                 ;; !!! What should I return here? A special slot-unbound value is needed. 
                 :null)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Instantiation

;; default (which applies to numbers)
;; Is this at all useful? I'm not sure.
(defmethod primary-key-value-using-class (class object)
  (declare (ignore class))
  object)

;; !!! This doesn't account for compound primary keys yet
(defmethod primary-key-value-using-class ((class postgres-class) object)
  (let ((key (primary-key class)))
    (slot-value object (first key))))

(defun primary-key-value (object)
  (primary-key-value-using-class (class-of object) object))

(defun relation-equal (a b)
  (and (eq (class-of a)
           (class-of b))
       (equal (primary-key-value a)
              (primary-key-value b))))

(defun simple-primary-key (class)
  (let ((relevant-supers (remove-if-not (lambda (class)
                                          (typep class 'postgres-class))
                                        (ccl:class-direct-superclasses class))))
    (if relevant-supers
        (find nil (mapcar #'simple-primary-key relevant-supers)
              :test-not #'eql)

        (and (slot-boundp class 'primary-key)
             (not (cdr (primary-key class)))
             (first (primary-key class))))))


(defun fk-slot-p (slot-definition)
  (let ((type (ccl:slot-definition-type slot-definition)))
    (and (symbolp type)
         (awhen (find-class type nil)
           (typep it 'postgres-class)))))

(define-presentation-method present ((object integer) (type date-and-time) (stream stream) (view sql-view)
                                     &key acceptably)
  acceptably ;; how do I make it (un)acceptable?
  (multiple-value-bind (second minute hour day month year)
      (decode-universal-time object)
    (format stream "~A-~2,'0D-~2,'0D ~2,'0D:~2,'0D:~2,'0D" year month day hour minute second)))

;; (present-to-string (get-universal-time) 'date-and-time :view +sql-view+)

(define-presentation-method present ((object integer) (type date) (stream stream) (view sql-view)
                                     &key acceptably)
  acceptably ;; how do I make it (un)acceptable?
  (multiple-value-bind (second minute hour day month year)
      (decode-universal-time object)
    (declare (ignore second minute hour))
    (format stream "~A-~2,'0D-~2,'0D" year month day)))
;; (present-to-string (get-universal-time) 'date :view +sql-view+)

(define-presentation-method present ((object t) (type t) (stream stream) (view sql-view)
                                     &key acceptably)
  acceptably
  (format stream "~A" object))

;; (present-to-string 123 'integer :view +sql-view+)
;; (present-to-string "hi there" 'string :view +sql-view+)
;; (present-to-string 0.123 'number :view +sql-view+)

;; NOTE: I'm not going to define one of these for floats because we shouldn't be using them at all
(define-presentation-method present ((object rational) (type t) (stream stream) (view sql-view)
                                     &key acceptably)
  acceptably
  (format stream "~A" (vip-utils:large-decimal-expansion object)))
;; (present-to-string 1/3 t :view +sql-view+)
;; (present-to-string 175/10 t :view +sql-view+)

(define-presentation-method present ((object t) (type boolean) (stream stream) (view sql-view)
                                     &key &allow-other-keys)
  (write-sequence (if object "true" "false")
                  stream))



(defun pg-value-for-slot (slot-definition value)
  (if (fk-slot-p slot-definition)
      (primary-key-value value)
      (if (slot-boundp slot-definition 'presentation-type)
          (present-to-string value (slot-definition-presentation-type slot-definition) :view +sql-view+)
          ;; This will be handled by the sql-literal function, which will be the right thing to do
          value)))


(defmethod expression-to-sql :around (x)
  (if (typep (class-of x) 'postgres-class)
      (if (simple-primary-key (class-of x))
          (call-next-method (primary-key-value x))
          (error "Cannot use object without simple primary key in SQL expression"))
      (call-next-method)))


;; This is cool - it completely removes the need to write explicit save methods like I did for the email-message class
;; FIXME (and continue with class interface)
(defmethod make-instance :around ((class postgres-class) &rest initargs &key &allow-other-keys)
  (declare (ignore initargs))
  ;; NOTE: we have to suppress insertions in case the constructor sets slot values
  ;; we want the object to be fully set up *before* we persist it to the database
  (let ((instance (let ((*suppress-database-insertion* t))
                    (call-next-method))))
    
    (unless *suppress-database-insertion*
      ;; because we might visit the same class more than once, we must remember not to repeatedly insert into the same table
      (let ((inserted (make-hash-table)))
        (labels ((insert-instance-of (class)
                   (let ((db-generated-slots (mapcar #'ccl:slot-definition-name
                                                     (vip-utils:find-all t
                                                                         (ccl:class-direct-slots class)
                                                                         :key #'relations::slot-definition-database-generated))))
                     (or (gethash (source-table class) inserted)
                         (let ((expression (append (list (source-table class))
                                                   (loop for super in (ccl:class-direct-superclasses class)
                                                      for pg = (typep super 'postgres-class)
                                                      when pg
                                                      collect (intern (symbol-name (source-table super)) :keyword)
                                                      when pg
                                                      collect
                                                        (insert-instance-of super))
                                                   ;; now go through my slots...
                                                   (loop for slot in (ccl:class-direct-slots class)
                                                      for slot-name = (ccl:slot-definition-name slot)
                                                      for boundp = (slot-boundp instance slot-name)
                                                      when boundp
                                                      collect (intern (symbol-name slot-name) :keyword)
                                                      when boundp
                                                      collect (pg-value-for-slot slot (slot-value instance slot-name))))))
                           ;; If there are no direct slots for this class there is nothing to do here
                           (let ((simple-pk (awhen (simple-primary-key class)
                                              ;; it has to be defined on /this/ class - otherwise I'll just ignore it
                                              ;; (the generated id will be handled in making the row for the superclass
                                              ;; - this caused a nasty error which DK spotted)

                                              ;; the only sort of problem here is that we can't let the DB handle enforcing
                                              ;; references to a subclass of another postgre-class by declaring an FK reference
                                              ;; to the subclass table. That would be nice. Probably not a big problem most of
                                              ;; the time? It can be enforced in CLOS of course.
                                              (when (find it (ccl:class-direct-slots class) :key #'ccl:slot-definition-name)
                                                it))))
                             (when (and (or (cdr expression)
                                            simple-pk)
                                        (not (dynamically-generated-class-p class)))
                               (let* ((returns (append (when simple-pk
                                                         (list simple-pk))
                                                       db-generated-slots))
                                      (values (if returns
                                                  (database-core:db-create expression
                                                                           :returning
                                                                           returns)
                                                  (database-core:db-assert expression))))
                                 (loop for v in (if simple-pk
                                                    (progn
                                                      (setf (gethash (source-table class) inserted)
                                                            (first values))
                                                      (cdr values))
                                                    values)
                                       for slot in db-generated-slots
                                       do (let ((*suppress-database-insertion* t))
                                            (setf (slot-value instance slot) v)))
                                 (when simple-pk
                                   (let ((*suppress-database-insertion* t))
                                     (setf (slot-value instance
                                                       simple-pk)
                                           (first values))))))))))))
          
          (insert-instance-of class))))
    instance))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; find-instance et al

(defmethod relation-for-instances ((class symbol))
  (relation-for-instances (find-class class)))

(defmethod relation-for-instances ((class postgres-class))
  class)

(defun key-value-constraint-expression (args)
  (let ((constraints (loop for (a b) on args by #'cddr
                        collect `(= (@ ,a) ,b))))
    (if (cdr constraints)
        `(and ,@constraints)
        (first constraints))))


(defmethod find-instance (x &rest args)
  (unique-tuple (if args
                    (select (if (symbolp (first args))
                                (key-value-constraint-expression args)
                                (first args))
                            (relation-for-instances x))
                    (relation-for-instances x))))

(defmethod find-instances (x &rest args)
  (tuples (if args
              (select (if (symbolp (first args))
                          (key-value-constraint-expression args)
                          (first args))
                      (relation-for-instances x))
              (relation-for-instances x))))

(defun get-instance (class &rest initargs)
  "Creates an instance of the given class with the id without fetching anything from the database.
Note the class MUST MUST have an init arg of :id,"
  (let ((*suppress-database-insertion* t))
    (apply #'make-instance class initargs)))


;; This gets an instance by id
;; to use it you MUST know that the instance exists
(defmethod instance-by-id ((x symbol) id)
  (instance-by-id (find-class x) id))

(defmethod instance-by-id ((class postgres-class) id)
  (let ((*suppress-database-insertion* t))
    (let ((object (allocate-instance class)))
      (setf (slot-value object 'id) id
            ;; code for demand loading:-
            ;; (slot-value object '.loaded) nil
            )
      object)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;

;; given a class and an instance of that class return a test to identifty the instance's row in the database
;; for a class which has superclasses this will need to be repeated...
(defmethod identifying-test-for-class ((class postgres-class) instance)
  (cond ((simple-primary-key class)
         `(= (@ ,(simple-primary-key class))
             ,(pg-value-for-slot (find (simple-primary-key class)
                                       (ccl:class-direct-slots class)
                                       :key #'ccl:slot-definition-name)
                                 (slot-value instance (simple-primary-key class)))))
        ;; !!! There should be a better way to do this test than using slot-boundp directly
        ((or (not (slot-boundp class 'primary-key))
             (not (primary-key class)))
         ;; Match on all the slots...
         `(and ,@(loop for slot in (ccl:class-direct-slots class)
                    collect `(= (@ ,(ccl:slot-definition-name slot))
                                ,(pg-value-for-slot slot
                                                    (slot-value instance
                                                                (ccl:slot-definition-name slot)))))
               ;; !!! Include all the FK references for superclasses too
               ,@ (loop for super in (ccl:class-direct-superclasses class)
                       when (typep super 'postgres-class)
                       collect `(= (@ ,(class-name super))
                                   ,(primary-key-value-using-class super instance)))))
        (t (error "Can't handle slot value setting with classes with compound primary keys yet"))))

;; (identifying-test-for-class (find-class 'test-broker) (find-instance 'co 'id 2))

(defun map-postgres-class-hierarchy (f class)
  (dolist (class (remove-if-not (lambda (x)
                                  (typep x 'postgres-class))
                                (ccl:class-precedence-list class)))
    (funcall f class)))

;; Now, if this object has a simple primary key we can use that to identify the correct row
;; if it does *not* then we can identify the row by specifying the entire 
;; !!! There's a bit more to this - superclasses etc. We have to look in the right table
(defmethod (setf ccl:slot-value-using-class) :before
    (new
     (class postgres-class)
     instance
     ;; !!! AMOP says this should be the slot NAME, not the slot definition
     (slotd ccl:standard-effective-slot-definition))
  (unless *suppress-database-insertion*
    (map-postgres-class-hierarchy
     (lambda (class)
       (when (member (ccl:slot-definition-name slotd)
                     (ccl:class-direct-slots class)
                     :key #'ccl:slot-definition-name)
         (setf (column-value (select (identifying-test-for-class class instance)
                                     (pg-table (source-table class)))
                             (ccl:slot-definition-name slotd))
               (pg-value-for-slot slotd new))))
     class)))

(defmethod database-value-for-type ((x t) (type t))
  "Translates an object x from the database into the required type"
  x)

(defmethod (setf ccl:slot-value-using-class) :around
    (new
     (class postgres-class)
     instance
     (slotd ccl:standard-effective-slot-definition))
  (if *suppress-database-insertion*
      (call-next-method (if (and (fk-slot-p slotd)
                                 (numberp new))
                            ;; !!! Is this equivalent? It means we don't really need to poke in the slot values
                            ;; (although it will still be a good idea)
                            (instance-by-id (ccl:slot-definition-type slotd) new)
                            (database-value-for-type new (ccl:slot-definition-type slotd)))
                        class instance slotd)
      (call-next-method)))


;; now we can lazily load the slot if it is an FK reference and isn't bound...
(defmethod ccl:slot-value-using-class :before
    ((class postgres-class)
     instance (slotd ccl:standard-effective-slot-definition))
  (let ((name (ccl:slot-definition-name slotd)))
    (when (and (not (slot-boundp instance name))
               (fk-slot-p slotd))
      ;; now we have to lazily initialize it...
      (let ((*no-fk-joins* nil)
            (*unjoined-classes* nil)
            (new (find-instance (class-name class)
                                (simple-primary-key class)
                                (primary-key-value instance))))
        (let ((*suppress-database-insertion* t))
          (loop for slot in (mapcar #'ccl:slot-definition-name
                                    (ccl:class-slots class))
                when (and (not (slot-boundp instance slot))
                          (slot-boundp new slot))
                  do (setf (slot-value instance slot)
                           (slot-value new slot))))))))

(defmethod ccl:slot-makunbound-using-class ((class postgres-class) instance (slotd ccl:standard-effective-slot-definition))
  (awhen (setf (ccl:slot-value-using-class (find-class 'standard-class) instance slotd) (ccl::%slot-unbound-marker))
    (database-core:dquery
     (format nil "UPDATE ~A SET ~A=NULL WHERE ~A=~A"
             (database-core:sql-name (source-table class))
             (database-core:sql-name (ccl:slot-definition-name slotd))
             (database-core:sql-name (simple-primary-key class))
             (database-core:sql-literal (primary-key-value instance))))
    it))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Deletion


;; This is now a method so that you can hook stuff in before/after it
(defmethod delete-object (object)
  (delete-object-using-class (class-of object) object))

;; given an instance delete it
(defmethod delete-object-using-class ((class postgres-class) object)
  ;; find out how to uniquely identify this instance with the class in question
  (map-postgres-class-hierarchy
   (lambda (class)
     (unless (dynamically-generated-class-p class)
       (delete-tuples (select (identifying-test-for-class class object)
                              (pg-table (source-table class))))))
   class))



;; this is going to be an analog for make-instance and takes similar parameters
(defun delete-instance (class &rest parameters)
  (declare (ignore class parameters))
  (error "Not implemented"))

;; Here we want to delete the whole instance, which means finding every table in which bits of it are stored
(defmethod delete-instance-using-class ((class postgres-class) object)
  (declare (ignore class object))
  (error "Not implemented yet"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; CHANGE CLASS

;; If I can handle this generally it will be useful so that I can (if necessary) add in history in postgres-class-with-logging...

;; I think I should be able to do this...
#+nil(defmethod ccl::change-class (old-instance (new-class postgres-class))
  (call-next-method)
  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; HISTORY!

;; This assumes that we only ever do things through this CLOS layer and we don't do database updates directly

(defclass postgres-class-with-logging (postgres-class)
  ((logging-table :initarg :logging-table :initform 'slot-history :reader logging-table)))

;; !!! This is untested AND we need to create the table
(defmethod (setf ccl:slot-value-using-class) :before
    (new
     (class postgres-class-with-logging)
     instance
     ;; !!! AMOP says this should be the slot NAME, not the slot definition
     (slotd ccl:standard-effective-slot-definition))
  (unless *suppress-database-insertion*
    (map-postgres-class-hierarchy
     (lambda (class)
       (when (member (ccl:slot-definition-name slotd)
                     (ccl:class-direct-slots class)
                     :key #'ccl:slot-definition-name)
         ;; log the change
         (database-core:db-assert (list (logging-table class)
                                        :table (symbol-name (source-table class))
                                        :object (primary-key-value instance)
                                        :slot (ccl:slot-definition-name slotd)
                                        :old (slot-value instance (ccl:slot-definition-name slotd))
                                        :new new))))
     
     class)))

;; If we want to do updates across multiple rows it's an altogether different issue.


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Since we will commonly want to present and accept things like this...

;; This class should be moved somewhere else...
;; !!! NOTE: this doesn't have an id slot, which is intentional. It must 
(defclass acceptable-object-with-id ()
  ())

;; although this slot isn't delcared above the assumption is there will be on in the postgres-class instance which is a subclass of this class. This avoids the need to declare a reader all the time
(defmethod id ((x acceptable-object-with-id))
  (slot-value x 'id))

(defmethod id-equal ((x acceptable-object-with-id) (y acceptable-object-with-id))
  (and (eq  (type-of x) (type-of y))
       (eql (id x)      (id y))))

(defun class-name-to-string (class-name)
  (string-capitalize (cl-ppcre:regex-replace-all "-" (symbol-name class-name) " ")))
;; (class-name-to-string 'postgres-class)

(defun class-name-to-regex (class-name)
  (cl-ppcre:regex-replace-all " " (class-name-to-string class-name) "\\s+"))
;; (class-name-to-regex 'postgres-class)

(defun subclass-names-to-regex (superclass)
  "Returns a regex looking for any of the subclasses of the given superclass."
  (format nil "^(?:~A)" (vip-utils:string-list (mapcar #'rel::class-name-to-regex
                                                                 (sort (mapcar #'class-name (ccl:class-direct-subclasses (find-class superclass)))
                                                                       (lambda (a b)
                                                                         (> (length (symbol-name a))
                                                                            (length (symbol-name b))))))
                                                         "|"))) 


;; !!! I would like to add some gubbins to present a name or whatever as well
;; I can put that in another class if needed
(define-presentation-method present ((x acceptable-object-with-id) (type acceptable-object-with-id)
                                     (stream stream) (view textual-view) &key &allow-other-keys)
  (format stream "~A ~A"
          (string-capitalize (cl-ppcre:regex-replace-all "-" (symbol-name (class-name (class-of x))) " "))
          (if (slot-boundp x 'id)
              (slot-value x 'id)
              ;; the following means it must be a prototype
              "???")))


;; (see 06-database-object - this should check all subclasses really)
;; (though we would have to deal with names which are subsets of other names
(define-parser-acceptor (x acceptable-object-with-id)
  (with-presentation-type-decoded (name)
      x
    (or (awhen (simple-parser:sp-scan (format nil "^~A" (class-name-to-regex name))
                                      :ignore-case t)
          (simple-parser:sp-skip-whitespace)
          (let ((object (find-instance name
                                       'id
                                       (or (simple-parser:sp-scan "^\\d+") (simple-parser:sp-error "Expected ID here")))))
            (simple-parser:sp-scan "^\\s*<[^>]*>")
            object))

        (loop for class in (ccl:class-direct-subclasses (find-class name))
           for a = (unless (dynamically-generated-class-p class)
                     (accept (class-name class) :stream :simple-parser))
           when a return a))))

(defmethod tuple-type ((x postgres-class)) (class-name x))

;; Obviously there is a trivial presentation type for any instance of postgres-class
(defmethod tuple-presentation-type ((x postgres-class))
  `(named-type ,(class-name x) ,(tuple-type x)))

;; (tuple-presentation-type (find-class 'v1:broker))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Convenience functions...

;; this is a simple notation for establishing one entity which has a selection of things related to it
(defun relate (referenced-as referent references)
  "Given a 'referent' this applies #'make-instance to all the lists
passed in as references (ie things which reference the referent)
appending the referenced-as parameter and the referent to each
list. This provides a convenient way to create an entity AND a set of
things which refer to it as one expression.

It returns the referent."
  (dolist (x references)
    (apply #'make-instance (append x (list referenced-as referent))))
  referent)


;; If querying, for example, quote-detail objects for a particular quote it's a good idea to suppress joining in quote and underwriter product
(defun insert-objects-for-slots (map &optional object)
  (flet ((update (object)
           (let ((*suppress-database-insertion* t))
             (loop for (path new-object) on map by #'cddr
                   do
                      (flet ((update-slot (object slot putative-value)
                               (setf (slot-value object slot)
                                     (let ((desired-id (slot-value (slot-value object slot) 'id)))
                                       (if (eql desired-id
                                                (slot-value putative-value 'id))
                                           putative-value
                                           ;; otherwsie we just have to do a database lookup to get it
                                           (find-instance (class-name (class-of putative-value))
                                                          :id desired-id))))))
                        (if (listp path)
                            (let ((target object))
                              (loop for (this . more) on path
                                    do
                                       (if more
                                           (setf target (slot-value target this))
                                           (update-slot target this new-object))))
                            (update-slot object path new-object)))))
           object))
    ;; as a convenience for mapping over things this kind of auto curries
    (if object
        (update object)
        (lambda (object)
          (update object)))))




;; For testing purposes I'm going to make a protocol to generate a lisp expression for creating a particular object
;; I will provide a general implementation...

(defparameter *seen-objects* nil)

;; map for restoring objects
(defparameter *map* nil)
(defparameter *did-output-map* nil)

;; by wrapping the slot access like this we can remove personal data during extraction
(defmethod creation-expression-slot-value (object slot-name)
  (slot-value object slot-name))

;; #'creation-expression-for-object-of-class
(defmethod creation-expression-for-object-of-class (object (class standard-class))
  ;; figure out which slots we can initialize 
  `(make-instance ',(class-name class)
                  ,@(loop for slot in (ccl:class-slots class)
                          for initarg = (first (ccl:slot-definition-initargs slot))
                          for name = (ccl:slot-definition-name slot)
                          for include = (and initarg (slot-boundp object name)
                                             (not (eql initarg :.loaded))
                                             (not (eql (ccl:slot-definition-name slot) 'id)))
                          when include collect initarg
                            when include
                              collect (creation-expression (creation-expression-slot-value object name)))))


(defmethod referencing-classes ((x t)) nil)

(defmethod find-references (referring-class refering-to &optional all)
  (declare (ignore all))
  (find-instances referring-class
                  (ccl:slot-definition-name
                   (find-if (lambda (slot)
                              ;; (break "SLot: ~A type ~A" slot (ccl:slot-definition-type slot))
                              (and (symbolp (ccl:slot-definition-type slot))
                                   (typep (find-class (ccl:slot-definition-type slot)) 'rel:postgres-class)
                                   (subtypep (class-name (class-of refering-to))
                                             (ccl:slot-definition-type slot))))
                            (ccl:class-slots (find-class referring-class))))
                  refering-to))

;; (find-references 'v1::cover-duration (rel:find-instance 'v1:underwriter-product :id 8117))
;; (find-references 'v1::cover-duration (rel:find-instance 'v1:underwriter-product :id 6399))

(defmethod object-key (x)
  (when (and (typep (class-of x) 'postgres-class)
             (find 'id (ccl:class-slots (class-of x))
                   :key #'ccl:slot-definition-name))
    (cons (class-name (class-of x)) (id x))))

(defmethod creation-expression-for-object-of-class (object (class postgres-class))
  (let ((refs (reduce #'append
                      (mapcar (lambda (ref-class)
                                (find-references ref-class object))
                              (referencing-classes object))))
        (object-key (object-key object)))
    (let ((this (call-next-method object class)))
      (when object-key
        (setf (gethash object-key *seen-objects*) t))
      (if refs
          `(awhen ,this
             ,@ (when object-key
                  `((setf (gethash ',object-key *map*) it))) 
             ,@(mapcar #'creation-expression refs)
             it)
          ;; I have to put this in anyway, because other things might refer to this from outside here
          (if object-key
              `(setf (gethash ',object-key *map*) ,this)
              this)))))




(defmethod creation-expression-for-object-of-class (object class)
  (declare (ignore class))
  object)


(defmethod creation-expression ((object t))
  (if (and *seen-objects*
           (object-key object)
           (gethash (object-key object) *seen-objects*))
      `(gethash ',(object-key object) *map*)
      (creation-expression-for-object-of-class object (class-of object))))



;; #'creation-expression-for-object-of-class
(defmethod creation-expression-for-object-of-class :around (object (class postgres-class))
  (labels ((f ()
             (if *seen-objects*
                 (call-next-method object class)
                 (let ((*seen-objects* (make-hash-table :test 'equal)))
                   (f)))))
    (if (or *did-output-map* (not (object-key object)))
        (f)
        (let ((*did-output-map* t))
          `(let ((*map* (make-hash-table :test #'equal)))
             ,(f))))))


(defmethod ccl:slot-makunbound-using-class ((class postgres-class) instance (slotd ccl:standard-effective-slot-definition))
  (awhen (setf (ccl:slot-value-using-class (find-class 'standard-class) instance slotd) (ccl::%slot-unbound-marker))
    (database-core:dquery
     (format nil "UPDATE ~A SET ~A=NULL WHERE ~A=~A"
             (database-core:sql-name (source-table class))
             (database-core:sql-name (ccl:slot-definition-name slotd))
             (database-core:sql-name (simple-primary-key class))
             (database-core:sql-literal (primary-key-value instance))))
    it))




;; I don't know if I should do more to generalise this concept.
;; I think it works to determine if two relations are equal

(defun relation-identity (obj)
  (awhen (and (typep (class-of obj) 'postgres-class)
              (simple-primary-key (class-of obj)))
    (cons (class-name (class-of obj))
          (slot-value obj it))))

(defun is-equal (a b)
  (let ((id-a (relation-identity a))
        (id-b (relation-identity b)))
    (and id-a id-b
         (equal id-a id-b))))
