# SQLSim

SQL Simulator in Javascript. Performance and storage space be damned! This is an in-memory simulator meant to show how SQL works, allowing inspection of database state at any (and every!) time during execution. 

### Supported SQL Flavors 

- [x] MySQL

More to come! 

### Keywords Supported 

- [x] CREATE TABLE (see below)
- [x] INSERT INTO
- [x] SELECT * FROM Table (basic selection)
- [x] SELECT _col1_, _col2_, ... FROM Table (column filtering via projection)
- [x] SELECT _col3_, _col3_, _col3_ FROM Table (column expansion via projection)
- [ ] SELECT _(col2 + 5)_ FROM Table (expressions via projection)
- [x] SELECT ... FROM Table WHERE _expression_ (row filtering via expressions, see below)
- [x] SELECT ... FROM Table JOIN AnotherTable ON _expression_ (inner joins)
- [x] SELECT ... FROM Table LEFT JOIN AnotherTable ON _expression_ (left joins)
- [x] SELECT ... FROM Table RIGHT JOIN AnotherTable ON _expression_ (right joins)
- [x] SELECT ... FROM Table FULL JOIN AnotherTable ON _expression_ (full joins)


#### CREATE TABLE 

- [ ] Data type constraints (e.g., INTEGER, VARCHAR(20), etc.)
- [ ] UNIQUE constraint
- [ ] NOT NULL constraint
- [ ] PRIMARY KEY constraint
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