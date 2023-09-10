import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

let storage = new Storage();

execute(`
CREATE TABLE T (
  age INTEGER
);

INSERT INTO T VALUES (20), (21);

SELECT * FROM T;
`, storage);

console.log(storage.databases['default'].tables['T'].rows[0].cells)

