(in-package :relations)




(defclass rdf-class-slot-definition (ccl:standard-slot-definition)
  ((subject :initarg :subject 
            :accessor slot-definition-subject)))

(defclass rdf-class (mapping standard-class)
  ((primary-key :initarg :primary-key
                :reader rdf-primary-key)
   (type :initarg :type
         :reader rdf-type)))

(defclass rdf-class-direct-slot-definition (ccl:standard-direct-slot-definition 
                                            rdf-class-slot-definition)
  ())

(defclass rdf-class-effective-slot-definition (ccl:standard-effective-slot-definition 
                                               rdf-class-slot-definition)
  ())

(defclass string-triple ()
  ((id :reader id)
   (subject :initarg :subject
            :reader string-triple-subject)
   (predicate :initarg :predicate
              :reader string-triple-predicate)
   (object :initarg :object
           :reader string-triple-object))
  (:primary-key id)
  (:metaclass postgres-class))




(defmethod ccl:validate-superclass ((class rdf-class) (super-class standard-class))
  t)

(defmethod ccl:direct-slot-definition-class ((class rdf-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'rdf-class-direct-slot-definition))

(defmethod ccl:effective-slot-definition-class ((class rdf-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'rdf-class-effective-slot-definition))

(defmethod ccl:compute-effective-slot-definition :around ((class rdf-class) name direct-slots)
  (declare (ignore name))
  (let ((definition (call-next-method)))
    (setf (slot-definition-subject definition) 
          (slot-definition-subject (first direct-slots)))
    definition))





(defmethod query-for-class ((class rdf-class))
  "Creates a query that joins in a string triple for each column in the class."
  (let ((primary-key (first (rdf-primary-key class)))
        (slots (ccl:class-slots class))
        (base (project (list `((@ subject) base-subject))
                       (select `(and (= (@ predicate) "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                 (member (@ object)
                                         (list ,(first (rdf-type class)))))
                               (pg-table 'uri-triple)))))
    (let ((r base))
      (loop for prop in slots
         do (setf r (left-join `(and (= (@ l base-subject) (@ r subject))
                                     (= (@ r predicate) ,(slot-definition-subject prop)))
                               (rename 'l r)
                               (rename 'r (project (list '((@ subject))
                                                         '((@ predicate))
                                                         `((@ object) ,(ccl:slot-definition-name prop)))
                                                   (pg-table 'string-triple))))))
      (project (mapcar (lambda (slot) 
                         (let ((name (ccl:slot-definition-name slot)))
                           (if (eq name primary-key)
                               `((@ base-subject) ,primary-key)
                               `((@ ,name))))) 
                       slots)
               r))))


(defmethod r0 ((class rdf-class))
  (query-for-class class))

(defmethod mapping-function ((x rdf-class))
  (lambda (relation)
    (declare (ignore relation))
    (lambda (row)
      (let ((*suppress-database-insertion* t))
        (let ((obj (make-instance x)))
          (loop for value in row
             for slot in (ccl:class-slots x)
             do
               (setf (slot-value obj (ccl:slot-definition-name slot))
                     value))
          obj)))))

(defmethod relation-columns ((r rdf-class))
  (relation-columns (r0 r)))

(defmethod relation-for-instances ((class rdf-class))
    class)


(defmethod attribute-values-for-relation (x (class rdf-class))
  (loop for slot in (mapcar #'ccl:slot-definition-name (ccl:class-slots class))
     collect (if (slot-boundp x slot)
                 (slot-value x slot)
                 ;; !!! What should I return here? A special slot-unbound value is needed. 
                 :null)))
