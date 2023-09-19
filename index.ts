import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

let storage = new Storage();

let results = execute(`
  CREATE TABLE People (
    name VARCHAR(20),
    age INTEGER,
    country_id INTEGER
  );

  CREATE TABLE Countries (
    country_id INTEGER,
    country_name VARCHAR(20)
  );

  INSERT INTO People VALUES ('Tim', 30, 1), ('Liz', 21, 2), ('Russ', 20, 3);
  INSERT INTO Countries VALUES (1, 'USA'), (2, 'Canada'), (3, 'Mexico');
  
  SELECT * FROM People
    JOIN (SELECT * FROM Countries) as C ON People.country_id = C.country_id;
`, storage);

//console.log(JSON.stringify(results, null, 2));

