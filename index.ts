import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

let storage = new Storage();

let results = execute(`
  CREATE TABLE T (
    age INTEGER
  );

  INSERT INTO T VALUES (20), (21);

  SELECT * FROM T;
`, storage);

console.log(JSON.stringify(results, null, 2));

