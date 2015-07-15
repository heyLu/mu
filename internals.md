# Internals

`mu` internals, which are likely also Datomic internals.

for now, just a long list, ordered loosely by concepts/terms.

- entity ids
    - partition + offset
    - based on T of the transaction they were created
    - assignment:
        - tx-id = `(tx-part * (1 << 42)) + db.NextT()`
        - sequential-id = db.NextT() + position of (new) entity in
            transaction (first position is 1)
        - partition is `(part * (1 << 42))`
        - entity id is `partition + sequential-id`
- transactions
    - entity and tx id assignment as described above
    - automatic retractions added for new values of `:db.cardinality/one` attributes
    - noop datoms are dropped  (retractions with non-matching values or
        of non-existing entities, assertions of the same value)
- representation in storage
    - "root": index root, log root, log tail
    - index root has uuids/segment names of the various indexes
    - log root containes indexed values from tx log
    - log tail contains not yet indexed tx values 
- log
    - log root/indexed log: TODO
    - log tail
        - `[]byte`, fressian encoded and gzipped
        - contains list of log txs in transaction order
        - `[log-tx*]`
    - log tx
        - `{:id uuid, :t int, :data datums}`
    - datum (only used in log, not sure why)
        - `#datum [added part id attribute value tx]`
- index
    - index root
        - map with keys, mostly names of segments containing roots
        - `:eavt`, `:aevt`, `:avet`, `:raet`: uuids of roots of current indexes (contains all asserted datoms from the last indexed transaction)
        - `-hist` variants of the index keys above, `nil` if no indexing yet, see asOf/since/history info below
        - `-mid` variants of the indexes, never seen them non `nil` yet, probably takes a while.  very likely for "not too old" values from the historical part of the index
        - `:fulltext` and `:fulltext-hist`, seems to be a lucene indexed mapped to segments in the store
        - `:nextT` of the db
        - the `:version` of the index storage format
        - `:rev` probably for coordination/"clustering"
        - `:buildRevision` build id, from datomic version (e.g. 5130 if version was 0.9.5130)
    - `index-root-node`
        - the root of an index (e.g. `:eavt`, `:aevt`, ...)
        - `#index-root-node [index-tdata segments]`
        - segments is a vector of uuids, refer to `index-dir-node`s
        - tdata used to find the corresponding segment (`length(index-tdata) == length(segments)`)
    - `index-dir-node`
        - `#index-dir-node [index-tdata segments starts lengths]`
        - components are vectors of the same length
        - segments are uuids, refer to `index-tdata` containing the actual values of the datoms
    - `index-tdata`
        - `#index-tdata [values eids attrs txs addeds]`
        - likely stands for "transposed" data
            - for faster lookup and storage efficiency
            - eids, attrs, txs int arrays
            - addeds is a boolean array
            - values is a vector (with arbitrary values, although they are `:db/valueType` types, or nil)
        - txs are stored without tx part, i.e. `tx-id - (3 * (1 << 42))`
        - in sorting order of the containing index
- sorting
    - sorting by first three components as expected, i.e. compare, if not equal
        return value of comparison, else continue with next
    - txs (the T in the index names) are sorted in reverse, i.e. largest first,
        this allows `db.AsOf` and `db.History`  (see below)
    - values are sorted by *type*, as they appear in the db, i.e. sorted by `ident`
        of the `:db.type/...`.  e.g. `ref > keyword > long > string > ...`
- asOf, since, history
    - asOf is essential a filter of in-memory + current + history, with values
        newer than `asOf` filtered by the iterator
    - since is similar, but has only newer values
    - history does not drop retractions
    - regarding retractions:  both the in-memory and the history index contain
        retractions as well as assertions.  when iterating these are usually
        dropped, except with a `history` db
        - retractions can be dropped because reverse sorting by T as last
            component means that a retraction and it's previous assertion
            always appear consecutively.  this means that when we ignore
            retractions we can skip the next datom if we see a datom with
            `added == false`.
    - example: consider the following transactions

            ; transaction #0
            [100 :name "Jane"      0 true]

            ; transaction #1, Jane changes her name
            [100 :name "Jane"      1 false]
            [100 :name "Jane Lane" 1 true]

            ; transaction #2, Jane changes her name again
            [100 :name "Jane Lane" 2 false]
            [100 :name "Jane L"    2 true]

    at `T=2` the eavt in-memory index looks as follows:

            [100 :name "Jane"      1 false]
            [100 :name "Jane"      0 true]
            [100 :name "Jane L"    2 true]
            [100 :name "Jane Lane" 2 false]
            [100 :name "Jane Lane" 1 true]

    when we iterate over it as usual, we drop the first two values,
    then yield `[100 :name "Jane L" 2 true]` and then drop the next
    two values.  (we drop both times because `added` is false.

    this results in the correct state, Jane's `:name` is "Jane L".

    now, let's imagine we use `db.AsOf(1)`, the index is still the
    same, but we ignore datoms whose T component is greater than 1.

    TODO: there's a bug, only drop datoms if the tx match.  (but
    first verify that it's really a bug.)
- backup
    - directory, with two subdirectories `roots` and `values`
    - `roots` contains files whose name is the T component of the most recent value in the db
    - `values` contains segments, with one additional subdirectory for the last two chars `<last-2-chars>/<full-uuid>`
    - everything encoded with fressian, values are gzipped in addition  (similar to db in storage)
    - [can be used as a read-only db, only supported by `mu`]
- in-memory index
    - read from the log tail
      
