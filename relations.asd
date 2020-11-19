
(asdf:defsystem #:relations
  :description "First class relations in CL using CLOS"
  :author "VIP"
  :license "vip"
  ;; Depend on pg? I could split out pg-relations into another package depending on this
  ;; let's start like this
  :depends-on ("database-core" "vip-utils" "vip-clim-core" "cl-ppcre" "simple-parser" "anaphors"
                               "clim-web")
  :serial t
  :components ((:file "package")
               (:file "base-classes")
               (:file "general-operations")
               (:file "postgres-interface")
               (:file "postgres-class")
               (:file "web-presentations")
               (:file "rdf-class" :depends-on ("postgres-class"))
               #+ponk(:file "rdf-member-class" :depends-on ("postgres-class" "rdf-class"))
               ))


