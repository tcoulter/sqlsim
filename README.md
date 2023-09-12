# SQLSim

SQL Simulator in Javascript. Performance and storage space be damned! This is an in-memory simulator meant to show how SQL works, allowing inspection of database state at any (and every!) time during execution. 

### Supported SQL Flavors 

- [x] MySQL

More to come! 

### Keywords Supported 

#### General

- [x] CREATE TABLE (see below)
- [x] INSERT INTO Table VALUES ... (all-column insert)
- [ ] INSERT INTO Table _(col1, col2, ...)_ VALUES ... (column-specific inserts)
- [x] SELECT * FROM Table (basic selection)
- [x] SELECT _col1_, _col2_, ... FROM Table (column filtering via projection)
- [x] SELECT _col3_, _col3_, _col3_ FROM Table (column expansion via projection)
- [x] SELECT _(col2 + 5)_ FROM Table (expressions via projection)
- [x] SELECT ... FROM Table WHERE _expression_ (row filtering via expressions, see below)
- [x] SELECT ... FROM Table JOIN AnotherTable ON _expression_ (inner joins)
- [x] SELECT ... FROM Table LEFT JOIN AnotherTable ON _expression_ (left joins)
- [x] SELECT ... FROM Table RIGHT JOIN AnotherTable ON _expression_ (right joins)
- [x] SELECT ... FROM Table FULL JOIN AnotherTable ON _expression_ (full joins)
- [ ] AS for table names
- [ ] AS for column names
- [ ] SELECT ... FROM Table WHERE _(SELECT ...)_ (subqueries in the WHERE clause)
- [ ] SELECT ... FROM _(SELECT ...)_ (subqueries in the FROM clause)
- [ ] SELECT _(SELECT ...) FROM ... (subqueries in the projection list)
 

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
- [ ] _IS_ operator
- [ ] _IS NOT_ operator
- [ ] _IN_ operator
- [ ] _NOT IN_ operator
- [ ] _LIKE_ operator