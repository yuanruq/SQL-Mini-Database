SQL-Mini-Database
=================
MDB is a miniature database system that supports a useful subset of SQL.  The primary limitations of MDB are that only string and integer types are supported, there are no aggregations, and selection predicates must be conjunctive.

MDB supports the following DDL commands:
Create -- create a relation.
Index -- create an index
Show -- print the attributes of the given relation.

MDB supports the following DML commands:
Abort   -- rollback all updates since the last abort or commit.
Close -- close a database that has no uncommitted updates.  Uncommitted updates must be aborted or committed prior to a close.
Commit -- commit all updates since the last abort or commit.
Delete -- delete the tuples associated with a single-relation (i.e. non-join) predicate.
Exit -- exit MDB.  This will close the currently opened database.
Insert -- insert tuple into database.
Open -- open a database for update and retrieval.  Only one database can be open at any time.
Script -- run the script in the designated file.
Select -- retrieve tuples from one or more relations.
Update -- update zero or more tuples in a single relation.

The MDB is built upon Berkeley DB (BDB) Java Edition, a Java-based inverted file system. 

See http://www.cs.utexas.edu/users/dsb/cs386d/Projects/Projects2-4.html#Project3
