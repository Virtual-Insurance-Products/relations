
(in-package :relations)

;; this is an implementation of a base relvar for tables stored in a postgres database
(defclass postgres-table (explicitly-named join-parameter selectable projectable) ())

(defun pg-table (name)
  (make-instance 'postgres-table :name name))

;; This *could* be made more specific I guess
(defmethod tuple-type ((x postgres-table))
  'list)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; QUERY GENERATION


(defmethod expression-operation-to-sql ((op (eql 'coalesce)) &rest operands)
  ;; !!! I'm definitely going to want to generalise this code
  (with-output-to-string (stream)
    (write-sequence "COALESCE(" stream)
    (loop for (this . rest) on operands
         do (write-sequence (expression-to-sql this) stream)
         when rest do (write-sequence ", " stream))
    (write-sequence ")" stream)))

;; these are both just unary operators
(defmethod expression-operation-to-sql ((op (eql 'exists)) &rest operands)
  ;; The 'in' (member) situation is a bit more complicated and, in general, we won't be able to operate on fixed mappings
  ;; - they will have to be reduced to simple projections. That will solve the problem
  (labels ((unwrap (x)
             (if (or (typep x 'projection)
                     (typep x 'fixed-mapping))
                 (unwrap (r0 x))
                 x)))
    (format nil "EXISTS ~A" (expression-to-sql (unwrap (first operands)))))
  #+nil(format nil "EXISTS ~A" (expression-to-sql (first operands))))

(defmethod expression-operation-to-sql ((op (eql 'not)) &rest operands)
  (format nil "(NOT ~A)" (expression-to-sql (first operands))))

(defmethod expression-operation-to-sql ((op (eql 'is-null)) &rest operands)
  (format nil "(~A IS NULL)" (expression-to-sql (first operands))))

(defmethod expression-operation-to-sql ((op (eql 'member)) &rest operands)
  (format nil "~A IN ~A"
          (expression-to-sql (first operands))
          (expression-to-sql (second operands))))

(defmethod expression-operation-to-sql ((op (eql 'list)) &rest operands)
  (format nil "(~{~a~^, ~})"
          (mapcar #'expression-to-sql operands)))

(defmethod expression-operation-to-sql (op &rest operands)
  (if (cdr operands)
      (with-output-to-string (stream)
        (write-sequence "(" stream)
        (loop for (this . rest) on operands
           do (write-sequence (expression-to-sql this) stream)
           when rest do (format stream " ~A " op))
        (write-sequence ")" stream))
      (format nil "~A (~A)" op (expression-to-sql (first operands)))))

(defmethod expression-operation-to-sql ((op (eql 'cast)) &rest operands)
  (format nil "(~A)::~A" (expression-to-sql (first operands))
          (second operands)))

(defmethod expression-to-sql (x)
  (database-core:sql-literal x))

(defmethod expression-to-sql ((x list))
  (if (expression-is-attribute-reference x)
      (let ((column-name (expression-column-name x)))
        ;; a list column name means that it's an object path, which SQL won't understand but it's useful to see it anyway
        (when (listp column-name)
          (setf column-name (format nil "...~A..." column-name)))
        (if (expression-table x)
            (format nil "~A.~A"
                    (database-core:sql-name (expression-table x))
                    (database-core:sql-name column-name))
            (database-core:sql-name column-name)))
      (apply #'expression-operation-to-sql x)))

;; Generalisation for nesting
(defmethod expression-to-sql ((x relation))
  (with-output-to-string (stream)
    (write-sequence "(" stream)
    (write-sql-query x stream)
    (write-sequence ")" stream)))


(defmethod write-sql-renamed-table ((x postgres-table) (stream stream))
  (write-sql-table-list x stream))

(defmethod write-sql-renamed-table (x (stream stream))
  (write-sequence "(" stream)
  (write-sql-query x stream)
  (write-sequence ")" stream))


(defmethod write-sql-table-list ((x postgres-table) (stream stream))
  (write-sequence (database-core:sql-name (name x)) stream))

(defmethod write-sql-table-list ((x cartesian-product) (stream stream))
  (write-sql-table-list (r0 x) stream)
  (write-sequence ", " stream)
  (write-sql-table-list (r1 x) stream))

(defmethod write-sql-table-list ((x sql-join) (stream stream))
  (write-sql-table-list (r0 x) stream)
  (format stream " ~A JOIN " (join-type x))
  (write-sql-table-list (r1 x) stream)
  (format stream " ON ~A" (expression-to-sql (join-condition x))))

(defmethod write-sql-table-list ((x renamed) (stream stream))
  (write-sql-renamed-table (r0 x) stream)
  (write-sequence " " stream)
  (write-sequence (database-core:sql-name (name x)) stream))

(defmethod write-sql-table-list ((x selection) (stream stream))
  (write-sql-table-list (r0 x) stream)
  (format stream " WHERE ~A" (expression-to-sql (expression x))))


(defmethod write-sql-projection-column ((x projection-column) (stream stream))
  (write-sequence (expression-to-sql (attribute-expression x)) stream)
  (when (name x)
    (format stream " ~A" (database-core:sql-name (name x)))))

(defun write-sql-projection-columns (x stream)
  (when x
    (write-sql-projection-column (first x) stream))
  (when (cdr x)
    (write-sequence ", " stream)
    (write-sql-projection-columns (cdr x) stream)))


(defmethod write-sql-query (x (stream stream))
  (write-sequence "SELECT * FROM " stream)
  (write-sql-table-list x stream))

(defmethod write-sql-query ((x projection) (stream stream))
  (write-sequence "SELECT " stream)
  (when (slot-value x 'distinct)
    (write-sequence "DISTINCT " stream))
  (write-sql-projection-columns (relation-columns x) stream)
  (write-sequence " FROM " stream)
  (write-sql-table-list (r0 x) stream))

(defmethod write-sql-query :after ((x grouping) (stream stream))
  (write-sequence " GROUP BY " stream)
  (write-sequence (expression-to-sql (group-expression x)) stream))

(defmethod write-sql-query ((x limited) stream)
  (write-sql-query (r0 x) stream)
  (format stream " LIMIT ~A" (limit-count x)))

(defmethod write-sql-query ((x ordered) stream)
  (write-sql-query (r0 x) stream)
  (format stream " ORDER BY ~A" (order-by-col x)))



;; we map tuples ourselves - do all the joins, selects and so forth in the database
(defmethod map-tuples-from-innermost-p ((r postgres-table)) t)


(defun sql-query (x)
  (with-output-to-string (stream)
    (write-sql-query x stream)))


;; Generally we can just get the SQL and map over that...
;; SO, the fact that the wrapping ultimately resulted in a pg-table (or more) means that we end up here in order to map
;; the tuples.
(defmethod map-tuples-of-relation ((f function) (table postgres-table) (r relation))
  ;; !!! this could be made to work properly using the same trick as sql-loop* and
  ;; new-map-month-end
  (let ((query (sql-query r)))
    (loop for row in (database-core:dquery query)
       do (funcall f row))
    #+nil(flet ((f ()
             (cl-postgres:exec-query
              (database-core::connection database-core::*database-connection*)
              query
              (cl-postgres:row-reader (fields)
                (loop while (cl-postgres:next-row)
                      do (funcall f (loop for field across fields
                                          collect (let ((value (cl-postgres:next-field field))) 
                                                    ;; Postmodern returns nil values as :null.
                                                    ;; pg would return nil, so Abel assumes this all over.
                                                    (unless (eq value :null)
                                                      value)))))))
             
             ))
      (if database-core::*database-connection*
          (f)
          (database-core::with-database
              (f))))))

;; (tuples (limit 10 (pg-table 'broker)))

(defun lisp-type-for-sql-type (sql-type)
  (case sql-type
    (20 'integer)
    (25 'string)
    (16 'boolean)
    (t t)))



(defmethod relation-columns-of-relation ((x postgres-table) (r relation))
  (loop for (name type)                 ; ignore the 3rd thing
     in (database-core:pg-result
         (database-core:pg-exec database-core::*database-connection*
                                (sql-query (limit 0 r)))
                      :attributes)
     collect (make-instance 'relation-column
                            :name name
                            ;; !!! I need type translations for this to work well, although
                            ;; when I explicitly define classes I will use the types from there.
                            :type (lisp-type-for-sql-type type))))

;; (relation-columns-of-relation (pg-table 'broker) (pg-table 'broker))

(defmethod relation-columns-of-relation ((x postgres-table) (r postgres-table))
  (loop for c in (call-next-method)
       do (setf (slot-value c 'table) (name x))
       collect c))

(defmethod tuple-count ((x postgres-table))
  (tuple-count-of-relation x x))

(defmethod tuple-count-of-relation ((x postgres-table) (r relation))
  (database-core:dquery/1 (format nil "select count(*) from (~A) x" (sql-query r))))




;; These are intentionally quite limited in terms of what is possible.
;; (potentially more could be added, but it might not be very pointful)

(defmethod (setf column-value-of-relation) (new (object postgres-table) (outer postgres-table) column)
  (unless (equal (name object) (name outer))
    (error "Attempting to update uncertain table ~A ~A" object outer))

  (database-core:dquery
   (format nil "UPDATE ~A SET ~A = ~A"
           (database-core:sql-name (name object))
           (database-core:sql-name column)
           (database-core:sql-literal new))))


;; Here we will update just a selection of the given table
(defmethod (setf column-value-of-relation) (new (object postgres-table) (selection selection) column)
  (unless (equal (name object) (name (r0 selection)))
    (error "Attempting to update uncertain table ~A ~A" object selection))

  (database-core:dquery
   (format nil "UPDATE ~A SET ~A = ~A WHERE ~A"
           (database-core:sql-name (name object))
           (database-core:sql-name column)
           (database-core:sql-literal new)
           (expression-to-sql (expression selection)))))


(defmethod delete-tuples-of-relation ((object postgres-table) (outer postgres-table))
  (unless (equal (name object) (name outer))
    (error "Delete on ambiguous tables: ~A ~A" object outer))

  (database-core:dquery
   (format nil "DELETE FROM ~A" (database-core:sql-name (name object)))))


(defmethod delete-tuples-of-relation ((object postgres-table) (selection selection))
  (unless (equal (name object) (name (r0 selection)))
    (error "Delete on ambiguous tables: ~A ~A" object selection))

  (database-core:dquery
   (format nil "DELETE FROM ~A WHERE ~A"
           (database-core:sql-name (name object))
           (expression-to-sql (expression selection)))))





(defclass postgres-query (postgres-table)
  ((query :initarg :query :reader postgres-query-query)
   (name :initform (gensym))))

(defun pg-query (x &optional (name (gensym)))
  (make-instance 'postgres-query :query x :name name))

(defmethod write-sql-renamed-table ((x postgres-query) (stream stream))
  (format stream "(~A) " (postgres-query-query x)))

(defmethod write-sql-table-list :before ((x postgres-query) (stream stream))
  (write-sql-renamed-table x stream))

;; these should not support the above things
