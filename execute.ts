import {
  Parser, 
  AST, 
  Create as ASTCreate, 
  Insert_Replace as ASTInsert, 
  Select as ASTSelect,
  Column as ASTColumn
} from "node-sql-parser";
import Storage from "./storage";
import { CellData } from "./storage/cell";
import { Commit, getLatestCommit } from "./storage/commit";
import Table, { ComputedTable } from "./storage/table";

type TableSpecifier = {
  db: string,
  table: string, 
  as: string
};

// There's a lot to be desired from the AST types...
// We have to fill out some details here on our own. 

export type ColumnRef = {
  type: "column_ref",
  table: null, // TODO: Figure this one out
  column: string // column name
}

type CreateDefinition = {
  column: ColumnRef,
  definition: {
    dataType: any, // TODO: Fill this out
    suffix: Array<any> // TODO: Fill this out
  }, 
  resource: "column"
}


// The following are the top level types used throughout the executor

interface CreateTable extends ASTCreate {
  keyword: "table", // Force table keyword
  table: TableSpecifier[], // Make table required - I'm not sure why it's returned as an array,
  create_definitions: CreateDefinition[]
}

interface Insert extends ASTInsert {
  type: "insert",
  table: TableSpecifier[] // The original type appears to be set to any
};

interface Select extends ASTSelect {
  type: "select",
  from: TableSpecifier[], // This will need to change later wrt subqueries
  columns: ASTColumn[] | "*" // Remove the any[]
}

export default function execute(sql:string, storage?:Storage) {
  if (typeof storage == "undefined") {
    storage = new Storage();
  }

  const parser = new Parser();
  let ast = parser.astify(sql, {
    database: "MySQL"
  }); 
  if (Array.isArray(ast) == false) {
    ast = [ast] as Array<AST>;
  }

  console.log(JSON.stringify(ast, null, 2));

  let results = executeFromAST(ast as AST[], storage);

  let sanitizedResults:Array<Commit|Array<Array<CellData>>> = results.map((result) => {
    if ((result instanceof Table) == false) {
      return result as number;
    }

    return (result as Table).getData();
  });

  return {
    results: sanitizedResults,
    storage
  }
};

export function executeFromAST(ast:AST[], storage:Storage):Array<Commit|Table> {
  let results:Array<Commit|Table> = [];

  ast.forEach((line) => {
    let result:Commit|Table;

    switch(line.type) {
      case "create": 
        result = create(line, storage);
        break;
      case "insert": 
        result = insert(line as Insert, storage);
        break;
      case "select": 
        result = select(line as Select, storage);
        break;
      default: 
        throw new Error("Statement '"  + line.type.toUpperCase() + "' not yet supported.");
    }

    results.push(result);
  })

  return results;
}

function getDatabase(name:string, storage:Storage) {
  if (name == null) {
    name = storage.defaultDatabase;
  }

  return storage.getDatabase(name);
}

function getTable(specifier:TableSpecifier, storage:Storage) {
  let database = getDatabase(specifier.db, storage);
  return database.getTable(specifier.table);
}

// function getDatabase()

function create(ast:ASTCreate, storage:Storage):Commit {
  switch(ast.keyword) {
    case "table": 
      return createTable(ast as CreateTable, storage);
      break;
    default: 
      throw new Error("CREATE " + ast.keyword.toUpperCase() + " is not yet supported");
  }
};

function createTable(ast:CreateTable, storage:Storage):Commit {
  // I'm not sure why ast.table is an array, so we'll assume there's only one result for now
  let tableName = ast.table[0].table;
  let database = getDatabase(ast.table[0].db, storage);

  let columnNames:Array<string> = [];

  ast.create_definitions.forEach((createDefinition:CreateDefinition) => {
    columnNames.push(createDefinition.column.column);
  });

  database.createTable(tableName, columnNames);

  return getLatestCommit();
};

function insert(ast:Insert, storage:Storage):Commit {
  let table = getTable(ast.table[0], storage);

  if (ast.columns == null) {
    ast.values.forEach((item) => {
      // We can insert via an array without named columns. 
      let data:Array<CellData> = [];

      // TODO: This assumes raw data is in the insert values. 
      // TODO: Support expressions. 
      item.value.forEach((cellData) => {
        data.push(cellData.value);
      })
      
      table.insert(data);
    });
  } else {
    // TODO: Insert differently. 
    throw new Error("INSERTing only some columns not implemented yet")
  }

  return getLatestCommit();
};

function select(ast:Select, storage:Storage):Table {
  let database = getDatabase(ast.from[0].db, storage);
  let table = database.getTable(ast.from[0].table);

  let projectedColumns:Array<string>|undefined = undefined;
  
  // TODO: We're just gonna project all the columns whenever we find
  // a star (*). Makes coding easier but likely will incur a performance hit. 
  // 
  // Note that we're checking for the case where ast.columns is *. 
  // This looks like it won't ever happen, but it's an allowed possibility
  // by the type provided by the parser. Note that we'll instead get * as a 
  // column_ref that the below code expands out. 
  if (ast.columns != "*") {
    projectedColumns = [];

    ast.columns.forEach((column) => {
      if (column.expr.type == "column_ref") {
        let columnName = column.expr.column;

        if (columnName == "*") {
          (projectedColumns as Array<string>).push.apply(projectedColumns, table.columns);
        } else {
          (projectedColumns as Array<string>).push(columnName);
        }
      } else {
        throw new Error("Only column names are supported in SELECT right now.");
      }
    });
  }

  // Return a locked version of the table so this intsance will return 
  // no new data even if cell objects are updated. 
  return new ComputedTable(table, projectedColumns);
}