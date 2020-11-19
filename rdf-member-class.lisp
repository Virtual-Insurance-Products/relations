(in-package :relations)



(defclass rdf-member-class (postgres-class)
  ((predicate :initarg :predicate
               :reader rdf-predicate)))

(defclass rdf-member-class-slot-definition (postgres-class-slot-definition)
  ())

(defclass rdf-member-class-direct-slot-definition (ccl:standard-direct-slot-definition 
                                                   rdf-member-class-slot-definition)
  ())

(defclass rdf-member-class-effective-slot-definition (ccl:standard-effective-slot-definition 
                                                      rdf-member-class-slot-definition)
  ())

(defclass uri-triple ()
  ((id :reader id)
   (subject :initarg :subject
            :reader uri-triple-subject)
   (predicate :initarg :predicate
              :reader uri-triple-predicate)
   (object :initarg :object
           :reader uri-triple-object))
  (:primary-key id)
  (:metaclass postgres-class))



(defmethod ccl:validate-superclass ((class rdf-member-class) (super-class standard-class))
  t)

(defmethod ccl:direct-slot-definition-class ((class rdf-member-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'rdf-member-class-direct-slot-definition))

(defmethod ccl:effective-slot-definition-class ((class rdf-member-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'rdf-member-class-effective-slot-definition))

#+ponk(defmethod ccl:compute-effective-slot-definition :around ((class rdf-member-class) name direct-slots)
  (declare (ignore name))
  (let ((definition (call-next-method)))
    (setf (slot-definition-subject definition) 
          (slot-definition-subject (first direct-slots)))
    definition))

(defmethod source-relation ((self rdf-member-class))
  (select `(= (@ predicate) ,(rdf-predicate self))
          (pg-table 'uri-triple)))


(defmethod r0-join-condition ((from rdf-member-class) (to relation) slot-name primary-key)
  (let ((table-name (source-table from)))
    `(= (@ ,table-name ,slot-name)
        (@ ,primary-key))))

(defmethod r0-join-condition ((from rdf-member-class) (to rdf-class) slot-name primary-key)
  (let ((table-name (source-table from)))
    `(= (@ ,table-name ,slot-name)
        (@ ,primary-key))))

(defmethod relation-for-instances ((class rdf-member-class))
    class)


(defmethod print-object :around ((obj t) (s stream))
  (handler-case
      (call-next-method)
    (t (e)
      (declare (ignore e))
      (print-unreadable-object (obj s)
        (princ (type-of obj) s)))))



(defclass broker-member-person ()
  ((broker :type v1:broker)
   (member :type person))
  
  (:metaclass rdf-member-class)
  (:predicate "http://xmlns.com/foaf/0.1/member"))

(when nil
  (find-instances 'broker-member-person)

  (source-relation (make-instance 'rdf-member-class :predicate  "http://xmlns.com/foaf/0.1/member"))
  
  )

