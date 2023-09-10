import {Parser} from 'node-sql-parser';

const parser = new Parser();
const ast = parser.astify('SELECT * FROM t;'); // mysql sql grammer parsed by default

console.log(ast);