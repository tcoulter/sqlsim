import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

let storage = new Storage();

let results = execute(`
  CREATE TABLE People (
    name VARCHAR(20),
    age INTEGER,
    from_id INTEGER
  );

  CREATE TABLE Countries (
    country_id INTEGER,
    country_name VARCHAR(20)
  );

  INSERT INTO People VALUES ('Tim', 20, 1), ('Liz', 21, 2);
  
  SELECT from_id, AVG(age + 2) + 3 FROM People GROUP BY from_id;
`, storage);

//console.log(JSON.stringify(results, null, 2));

