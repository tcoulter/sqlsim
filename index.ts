import {AST, Parser} from 'node-sql-parser';
import Storage from "./storage";
import execute from "./execute";

const parser = new Parser();
let ast = parser.astify(
`
  CREATE TABLE T (
    age INTEGER
  );

  INSERT INTO T VALUES (20);
`, {
  database: "MySQL"
}); 

console.log(JSON.stringify(ast, null, 2));

let storage = new Storage();

if (Array.isArray(ast) == false) {
  ast = [ast] as Array<AST>;
}

execute(ast as Array<AST>, storage);

console.log(storage.databases['default'].tables['T'].rows[0].cells)

