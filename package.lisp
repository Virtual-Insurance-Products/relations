
(in-package :cl-user)

(defpackage :relations
  (:use :cl :clim-internals :anaphors
        :com.gigamonkeys.html
        :clim-web) ; should use more really. database-core for example
  (:nicknames :rel)
  (:export #:pg-table

           #:relation
           #:name
           
           #:relation-acceptable-string
           
           ;; relational operators
           #:limit
           #:order-by
           #:product
           #:select
           #:project
           #:group-by
           #:runion
           #:rename
           #:join
           #:cast ; cast SQL values
           
           #:left-join
           #:inner-join

           #:tuple-count
           #:columns
           #:tuples
           #:unique-tuple
           #:one-tuple
           #:map-tuples
           #:attribute-values
           #:delete-tuples

           #:find-instance
           #:find-instances
           #:get-instance
           #:delete-object
           #:delete-instance
           #:column-value

           #:relation-presentation-type

           #:postgres-class
           #:rdf-class
           
           #:without-fk-references

           #:acceptable-object-with-id
           #:id ; export from here so that id always refers to the same slot. It's used by the above...

           #:relation-equal

           #:coalesce
           #:exists

           #:is-null

           ;; some useful presentation types we define
           #:satisfies-expression
           #:tuple-from-relation
           #:new-slot-value-specification

           #:relate

           ;; A useful way of making a relation from a command which can be accepted
           #:relation-from-command

           ;; which wraps commands taking instances of postgres-class classes as commands taking those but wrapped in a 'tuple-from-relation type for security reasons
           #:wrap-command

           #:without-joining
           #:insert-objects-for-slots

           #:database-value-for-type

           #:creation-expression
           #:creation-expression-for-object-of-class
           #:object-key ; specify what to use in the marshalling map
           #:creation-expression-slot-value

           #:referencing-classes
           #:find-references

           #:is-equal
           )
  )
