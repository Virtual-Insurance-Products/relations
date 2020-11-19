
(in-package :relations)


;; Stuff for presenting relations nicely on web pages...
;; !!! Support 
(defclass relation-table-view ()
  ;; What shall I have as the representation for these?
  ;; Shall I use the same notation project uses or not? If I do then I can just project to get the columns, but I don't want to
  ;; change what the object is. 
  ((columns :initarg :columns)
   (column-headings :initarg :column-headings)
   ;; allow the view for the tuples to be specified too
   ;; (tuple-view )
   ;; sensitive rows? cells?

   ;; additional commands to show...

   ;; these 2 allows us to extend the relation by adding more things and then retract the tuples to get what the row
   ;; represents...
   (extended-relation :initform (lambda (rel) rel) :initarg :extended-relation :reader extended-relation)
   (retracted-tuple :initform (lambda (tuple) tuple) :initarg :retracted-tuple :reader retracted-tuple)

   (sort-function :initform nil :initarg :sort-function :reader sort-function)
   (sort-key :initform #'identity :initarg :sort-key :reader sort-key)))




(defmethod attribute-values-for-relation-with-view (tuple (relation relation) (view relation-table-view))
  (labels ((v (object slot)
             (when (slot-boundp object slot)
               (slot-value object slot)))
           (get-slot (object path)
             (if (symbolp path)
                 (v object path)
                 (if (cdr path)
                     (get-slot (v object (first path))
                               (cdr path))
                     (v object (first path))))))
    (if (slot-boundp view 'columns)
        (if (typep (class-of tuple) 'standard-class)
            ;; we can use slot-value
            (loop for col in (slot-value view 'columns)
               collect
                 (if (functionp col)
                     col ; just let it through - it should be a painting function
                     (get-slot tuple col)))
            (error "Don't know how to only get named values"))
        (attribute-values-for-relation tuple relation))))


(defmethod columns-for-view ((x relation) (view relation-table-view))
  (if (slot-boundp view 'columns)
      (let ((source (relation-columns x)))
        (loop for c in (slot-value view 'columns)
           collect (if (functionp c)
                       c
                       (find (if (symbolp c)
                                 (list c)
                                 c)
                             source :key #'name
                             :test #'equal))))
      (columns x)))


(define-presentation-method default-view-for-object ((x relation) (type relation) (stream keyword))
  (make-instance 'relation-table-view))


;; (define-presentation-method object-html-element ((object t) (type t) (view relation-row-view)) :tr)

;; we assume it is a tuple then...
;; ... TBC
#+nil(define-presentation-method present ((tuple t) (type t) (stream (eql :html)) (view relation-row-view)
                                     &key &allow-other-keys)

  (let* ((rel (slot-value view 'source-relation))
         (values (attribute-values-for-relation-with-view tuple rel view))
         (columns (slot-value view 'columns)))
    (with-output-as-presentation (:html tuple (presentation-type-of tuple) :element-type :tr)
      (loop for col in columns
         for value in values
         for ptype = (column-presentation-type col)
         do (html-present value ptype :element-type :td
                          ;; !!! We should have a way of specifying the view
                          :view (make-instance 'textual-view))))))

;; To make things editable:-
;; 1. for each column check to see if there is a command which takes parameters of EITHER:-
;;    (a) new-slot-value-specification, relation-table-view
;;    (b) new-slot-value-specification only
;; 2. if there is the show some editing thingy for the cell which invokes that command
;; 3. provide a function for use in those commands, when specialised with a view, which says 'redraw this object with this view'. Actually that seems familiar
;; 4. make a way to make the view acceptable so the command can be general...
;;    - this might involve having a registry of view
;;    - we might sometimes want to subclass relation-table-view and have a mechanism to register it with an id on first use. Maybe we'll defparamter it somewhere. Maybe that will instantiate its name. Maybe we'll just specify what to call it when we create it and it will register itself at the appropriate time. I can easily make nameable things like that (provided it isn't dangerous!)
;;    - I guess in principle the view could come from a command and we could do what I did with the broker lists


(define-presentation-method present ((rel relation) (type relation) (stream (eql :html)) (view relation-table-view)
                                     &key &allow-other-keys)
  (let* ((columns (columns-for-view rel view))
         (headings (if (slot-boundp view 'column-headings)
                       (slot-value view 'column-headings)
                       ;; The following isn't that good
                       (mapcar #'name columns))))
    (html
      (:table :class "clim sensitive-rows"
              (:thead
               (:tr
                (dolist (c headings)
                  (if (functionp c)
                      (funcall c)
                      (html (:td (:print
                                  (labels ((f (c)
                                             (cond ((and c
                                                         (listp c))
                                                    (format nil "~{~a~^ > ~}"
                                                            (mapcar #'f c)))
                                                   ((symbolp c)
                                                    (string-capitalize (symbol-name c)))
                                                   ((stringp c)
                                                    c))))
                                    (f c)))))))))
              
              (:tbody
               (mapc (lambda (tuple)
                       (let ((values (attribute-values-for-relation-with-view tuple rel view))
                             (rtuple (funcall (retracted-tuple view) tuple)))
                         (with-output-as-presentation (:html rtuple (presentation-type-of rtuple) :element-type :tr)
                           (loop for col in columns
                              for nil in headings
                              for value in values
                              do
                                (if (functionp col)
                                    (html (:td (funcall col tuple)))
                                    (if (eq value :null)
                                        (html (:td))
                                        (html-present value (column-presentation-type col)
                                                      :element-type :td
                                                      ;; !!! We should have a way of specifying the view
                                                      :view (make-instance 'textual-view))))))))
                     (awhen (tuples (funcall (extended-relation view) rel))
                       (if (sort-function view)
                           (sort it (sort-function view)
                                 :key (sort-key view))
                           it))))))))



;; so, 

;; Should I make a simple list view? Maybe.
