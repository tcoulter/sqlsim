import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

let storage = new Storage();

let results = execute(`
  CREATE TABLE People (
    name VARCHAR(20),
    age INTEGER,
    dept INTEGER
  );

  CREATE TABLE Countries (
    country_id INTEGER,
    country_name VARCHAR(20)
  );

  INSERT INTO People VALUES ('Tim', 30, 1), ('Liz', 21, 2), ('Russ', 20, 2), ('Bob', 21, 1);
  
  SELECT from_id, AVG(age) FROM People GROUP BY from_id HAVING AVG(age) > 22;
`, storage);

//console.log(JSON.stringify(results, null, 2));

