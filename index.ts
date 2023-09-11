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
  INSERT INTO Countries VALUES (1, 'USA'), (2, 'Canada');

  SELECT * FROM People JOIN Countries ON from_id = country_id;
`, storage);

//console.log(JSON.stringify(results, null, 2));

