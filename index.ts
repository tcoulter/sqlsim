import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

let storage = new Storage();

let results = execute(`
  CREATE TABLE People (
    name VARCHAR(20),
    age INTEGER
  );

  INSERT INTO People VALUES ('Tim', 20), ('Liz', 21);

  SELECT * FROM People WHERE age * 2 > 5.4;
`, storage);

//console.log(JSON.stringify(results, null, 2));

