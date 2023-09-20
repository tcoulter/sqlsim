# SQLSim

SQL Simulator in Javascript. Performance and storage space be damned! This is an in-memory simulator meant to show how SQL works, allowing inspection of database state at any (and every!) time during execution. 

### Install

Clone the repository then run the following commands: 

```
$ bun install
$ bun test
```

**Don't have bun?** You can get it [here](https://bun.sh/docs/installation). If you don't want to use bun, you can also easily use npm or yarn instead.  

### Supported SQL Flavors 

- [x] MySQL

More to come! 

### Keywords Supported 

#### General

- [x] CREATE TABLE (see below)
- [x] INSERT INTO Table VALUES ... (all-column insert)
- [x] INSERT INTO Table _(col1, col2, ...)_ VALUES ... (column-specific inserts)
- [x] UPDATE Table SET _col1 = ...; (update all records)
- [x] UPDATE Table SET _col1 = ..., col2 = ... WHERE ... (single and multi column updates with filtering)
- [x] SELECT * FROM Table (basic selection)
- [x] SELECT _col1_, _col2_, ... FROM Table (column filtering via projection)
- [x] SELECT _col3_, _col3_, _col3_ FROM Table (column expansion via projection)
- [x] SELECT _(col2 + 5)_ FROM Table (expressions via projection)
- [x] SELECT ... FROM Table WHERE _expression_ (row filtering via expressions, see below)
- [x] SELECT ... FROM Table JOIN AnotherTable ON _expression_ (inner joins)
- [x] SELECT ... FROM Table LEFT JOIN AnotherTable ON _expression_ (left joins)
- [x] SELECT ... FROM Table RIGHT JOIN AnotherTable ON _expression_ (right joins)
- [x] SELECT ... FROM Table FULL JOIN AnotherTable ON _expression_ (full joins)
- [x] SELECT ... FROM Table CROSS JOIN AnotherTable (cross join)
- [x] SELECT Table1.col1, Table2.col2 FROM Table1 JOIN Table2 ON ... (table name prefixing)
- [x] SELECT ... FROM Table _AS NewName_ (AS for table names)
- [x] SELECT col1 _AS newname_ FROM ... (AS for column names)
- [ ] SELECT _5 AS newname_ (selecting literals with AS for dymanic table creation)
- [x] SELECT ... FROM Table WHERE _(SELECT ...)_ (subqueries in the WHERE clause)
- [x] SELECT ... FROM Table WHERE _col1 > (SELECT AVG(col1) FROM ... WHERE col2 = Table.col2)_ (correlated subquery)
- [x] SELECT ... FROM _(SELECT ...)_ (subqueries in the FROM clause)
- [ ] SELECT _(SELECT ...) FROM ... (subqueries in the projection list)
- [x] SELECT ... FROM ... ORDER BY _col1 ASC, col2 DESC, ..._ (single and multi-column row ordering, with direction)
- [x] SELECT ... FROM ... ORDER BY _col1 > 5, ..._ (single and multi-column row ordering with expressions)
- [x] SELECT _FUNC(col1)_ FROM Table (simple aggregation)
- [x] SELECT _FUNC(col1 + 100)_ FROM Table (aggregation with expressions)
- [x] SELECT _FUNC(col1) + 100_ FROM Table (aggregation as expressions)
- [x] SELECT _groupCol, FUNC(col1)_ FROM Table GROUP BY (aggregation with grouping)
- [x] SELECT _groupCol, FUNC(col1)_ FROM Table GROUP BY ... HAVING ... (with grouping and filtering) 
 

#### CREATE TABLE 

- [ ] Data type constraints (e.g., INTEGER, VARCHAR(20), etc.)
- [ ] AUTO_INCREMENT constraint
- [ ] UNIQUE constraint
- [ ] NOT NULL constraint
- [ ] PRIMARY KEY _(col1)_ constraint (single primary key)
- [ ] PRIMARY KEY _(col1, col2, ...)_ constraint (composite primary key)
- [ ] FOREIGN KEY ... REFERENCES ... constraint
- [ ] ON UPDATE constraint
- [ ] ON DELETE constraint
- [ ] CHECK constraints

#### Expressions (e.g., in WHERE clause)

- [x] Column references (e.g., _age > ..._)
- [x] _+_ operator
- [x] _-_ operator
- [x] _/_ operator
- [x] _*_ operator
- [x] _=_ operator
- [x] _!=_ operator
- [x] _<_ operator
- [x] _<=_ operator
- [x] _>_ operator
- [x] _>=_ operator
- [x] _<>_ operator
- [x] _AND_ operator
- [x] _OR_ operator
- [x] _IS_ operator (Note: only TRUE/FALSE supported; UNKNOWN not yet supported)
- [x] _IS NOT_ operator (Note: only TRUE/FALSE supported; UNKNOWN not yet supported)
- [x] _IN (...expression list...)_ operator
- [ ] _IN (...subquery...)_ operator
- [x] _NOT IN (...expression list...)_ operator
- [ ] _NOT IN (...subquery...)_ operator
- [x] _LIKE_ operator
- [ ] _ANY_ operator
- [ ] _SOME_ operator

#### Aggregation Functions 

- [x] AVG()
- [ ] AVG(DISTINCT)
- [ ] BIT_AND()
- [ ] BIT_OR() 
- [ ] BIT_XOR()
- [x] COUNT()
- [ ] COUNT(DISTINCT)
- [ ] GROUP_CONCAT()
- [ ] GROUP_CONCAT(DISTINCT)
- [ ] JSON_ARRAYAGG() 
- [ ] JSON_OBJECTAGG()
- [x] MAX()
- [ ] MAX(DISTINCT)
- [x] MIN()
- [ ] MIN(DISTINCT)
- [ ] STD()
- [ ] STDDEV()
- [ ] STDDEV_POP()
- [ ] STDDEV_SAMP()
- [x] SUM()
- [ ] SUM(DISTINCT)
- [ ] VAR_POP()
- [ ] VAR_SAMP()
- [ ] VARIANCE()