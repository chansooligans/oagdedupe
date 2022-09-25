# Key Terms

.. list-table:: Terms
   :widths: 25 25 50
   :header-rows: 1

   * - Term
     - Definition
   * - Entity
     - a record in a dataset, e.g. {"name":"John Adams", addr:"28 chery st"}  
   * - Attributes
     - the fields that will be used for deduplication, e.g. ("name", "address")
   * - comparison pairs
     - a pair of entity IDs that will be compared, e.g. if {28: "john", 24:"sarah"} thene (24,28) would be a 
     comparison pair
   * - block scheme
     - a function used for blocking, e.g. first_two_characters(firstname)
   * - signature
     - the output of a function on a field e.g. first_two_characters("john") = "jo"
   * - forward index
     - a mapping from entity to signature; mappings can be concatenated to a dataframe 
     where rows represent entities, columns are block schemes, and values are signatures
   * - inverted index
     - a mapping from signature to an array of entities that share the signature, e.g. if {28:"john", "30":"joe", 24:"sarah"} then {"jo":[28,30], "sa":[24]} is the inverted index
   * - block conjunction
     - a conjunction of block schemes, e.g. "first 2 characters of name" AND "exact match on postcode" AND "common 
     acronym"
   * - reduction ratio
     - the number of comparisons omitted from blocking divided by the total possible number of comparisons that would be 
     made without blocking
   * - coverage
     - "positive coverage" is the percentage of samples labeled as "match" that are "covered" by the blocking conjunction, where "covered" means that applying the blocking conjunction yields comparison pairs that contain the positively labeled sample. "negative coverage" is defined in the same way, except it is the percentage of samples labeled as "not a match" that are "covered"
