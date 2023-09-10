import {AST, Create, Insert_Replace, Parser} from "node-sql-parser";
import Storage from "./storage";
import { CellData } from "./storage/cell";
import { Commit, getLatestCommit } from "./storage/commit";

type TableSpecifier = {
  db: string,
  table: string, 
  as: string
};


// There's a lot to be desired from the AST types...
// We have to fill out some details here on our own. 

type CreateDefinition = {
  column: {
    type: "column_ref",
    table: null, // TODO: Figure this one out
    column: string // column name
  }, 
  definition: {
    dataType: any, // TODO: Fill this out
    suffix: Array<any> // TODO: Fill this out
  }, 
  resource: "column"
}


// The following are the top level types used throughout the executor

interface CreateTable extends Create {
  keyword: "table", // Force table keyword
  table: TableSpecifier[], // Make table required - I'm not sure why it's returned as an array,
  create_definitions: CreateDefinition[]
}

interface Insert extends Insert_Replace {
  type: "insert",
  table: TableSpecifier[] // The original type appears to be set to any
};

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

  let commits = executeFromAST(ast as AST[], storage);

  return {
    commits,
    storage
  }
};

export function executeFromAST(ast:AST[], storage:Storage) {
  let commits:Array<Commit> = [];

  ast.forEach((line) => {
    switch(line.type) {
      case "create": 
        create(line, storage);
        break;
      case "insert": 
        insert(line as Insert, storage);
        break;
      default: 
        throw new Error("Statement '"  + line.type.toUpperCase() + "' not yet supported.");
    }

    commits.push(getLatestCommit());
  })

  return commits;
}

function getDatabase(name:string, storage:Storage) {
  let databaseName = name;

  if (databaseName == null) {
    databaseName = storage.defaultDatabase;
  }

  return storage.getDatabase(databaseName);
}

function getTable(specifier:TableSpecifier, storage:Storage) {
  let database = getDatabase(specifier.db, storage);
  return database.getTable(specifier.table);
}

// function getDatabase()

function create(ast:Create, storage:Storage) {
  switch(ast.keyword) {
    case "table": 
      createTable(ast as CreateTable, storage);
      break;
    default: 
      throw new Error("CREATE " + ast.keyword.toUpperCase() + " is not yet supported");
  }
};

function createTable(ast:CreateTable, storage:Storage) {
  // I'm not sure why ast.table is an array, so we'll assume there's only one result for now
  let tableName = ast.table[0].table;
  let database = getDatabase(ast.table[0].db, storage);

  let columnNames:Array<string> = [];

  ast.create_definitions.forEach((createDefinition:CreateDefinition) => {
    columnNames.push(createDefinition.column.column);
  });

  let table = database.createTable(tableName, columnNames);
};

function insert(ast:Insert, storage:Storage) {
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
};