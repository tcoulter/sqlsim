import {
  Parser, 
  AST, 
  Create as ASTCreate, 
  Insert_Replace as ASTInsert, 
  Select as ASTSelect,
  Update as ASTUpdate,
  SetList as ASTSetList
} from "node-sql-parser";
import Storage from "./storage";
import { CellData } from "./storage/cell";
import { Commit, getLatestCommit } from "./storage/commit";
import Table, { AggregateTable, FilteredTable, JoinType, JoinedTable, LockedTable, ProjectedTable, RowFilter } from "./storage/table";
import compute, { AggregateExpression, BinaryExpression, Expression, Literal, SingleExpression, stringifyExpression } from "./compute";
import {debug} from "debug";

const debugAst = debug('ast');

type TableSpecifier = {
  db: string,
  table: string, 
  as: string,
  join?: "INNER JOIN",
  on?: BinaryExpression
};

type ColumnSpecifier = {
  expr: ColumnRef | BinaryExpression | AggregateExpression,
  as: string | null
}

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
  columns: ColumnSpecifier[] | "*" // Remove the any[]
  where: BinaryExpression | null,
}

interface Update extends ASTUpdate {
  type: "update", 
  table: TableSpecifier[], // Make table required - I'm not sure why it's returned as an array,
  where: BinaryExpression | null,
  set: Array<SetItem>
}

interface SetItem extends ASTSetList {
  column: string;
  value: SingleExpression;
  table: string | null;
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

  debugAst(JSON.stringify(ast, null, 2));

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
      case "update": 
        result = update(line as Update, storage);
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

  ast.values.forEach((item) => {
    // We can insert via an array without named columns. 
    let data:Array<CellData> = [];

    // TODO: This assumes raw data is in the insert values. 
    // TODO: Support expressions. 
    item.value.forEach((cellData) => {
      data.push(cellData.value);
    })
    
    table.insert(data, ast.columns != null ? ast.columns : undefined);
  });

  return getLatestCommit();
};

function update(ast:Update, storage:Storage):Commit {
  let table = getTable(ast.table[0], storage);

  let columns:Array<string> = []
  let values:Array<CellData|SingleExpression> = [];

  ast.set.forEach((item) => {
    columns.push(item.column);

    switch(item.value.type) {
      case "binary_expr":
      case "column_ref": 
        values.push(item.value);
        break;
      case "bool":
      case "null":
      case "number":
      case "single_quote_string":
        values.push((item.value as Literal).value);
        break;
      default:
        throw new Error("Unexpected type on right hand side of SET: " + (item.value as any).type)
    }
  });

  table.update(columns, values, ast.where != null ? ast.where : undefined);

  return getLatestCommit();
}

function select(ast:Select, storage:Storage):Table {
  let database = getDatabase(ast.from[0].db, storage);

  // Process source tables (e.g., joins) before doing anything
  let table = database.getTable(ast.from[0].table);

  for (var fromIndex = 1; fromIndex < ast.from.length; fromIndex++) {
    let newFromDefinition = ast.from[fromIndex];

    if (typeof newFromDefinition.join != "undefined") {
      table = new JoinedTable({
        type: newFromDefinition.join.toLowerCase().replace("join", "").trim() as JoinType,
        left: table,
        right: database.getTable(newFromDefinition.table),
        on: newFromDefinition.on as BinaryExpression
      });
    }
  }

  if (ast.where != null) {
    table = new FilteredTable({
      table,
      rowFilters: [createWhereFilter(ast.where)]
    })
  }

  

  // Do projection OR aggregation.
  // Note that we're checking for the case where ast.columns is *. 
  // This looks like it won't ever happen, but it's an allowed possibility
  // by the type provided by the parser. Note that we'll instead get * as a 
  // column_ref that the below code expands out. 
  if (ast.columns != "*") {
    // Check for aggregation
    let includesAggregation = false; 

    for (var i = 0; i < ast.columns.length; i++) {
      if (ast.columns[i].expr.type == "aggr_func") {
        includesAggregation = true;
        break;
      }
    }

    if (includesAggregation == true) {
      // Aggregation
      // TODO: Binary expressions with aggregate functions inside! 
      table = new AggregateTable(ast.columns.map((spec) => spec.expr as ColumnRef|AggregateExpression), table);
    } else {
      // Projection 
      let projectedColumns:Array<string> = [];
      let computedColumns:Record<string, BinaryExpression> = {};

      ast.columns.forEach((column) => {
        let columnName = "";
        switch(column.expr.type) {
          case "column_ref":
            columnName = column.expr.column;

            if (columnName == "*") {
              (projectedColumns as Array<string>).push.apply(projectedColumns, table.columns);
            } else {
              (projectedColumns as Array<string>).push(columnName);
            }
            break;
          case "binary_expr": 
            columnName = stringifyExpression(column.expr);
            (projectedColumns as Array<string>).push(columnName);
            computedColumns[columnName] = column.expr;
            break;
          default: 
            throw new Error("Unsupported column type in projection: " + (column.expr as any).type);
        }
      });

      table = new ProjectedTable({
        table,
        columns: projectedColumns,
        computedColumns
      })
    }
  }

  // Return a locked version of the table so this intsance will return 
  // no new data even if cell objects are updated. 
  return new LockedTable(table);
}

export function createWhereFilter(expr:BinaryExpression):RowFilter {
  return (filtredTable, filteredRow) => {
    // TODO(?): Notice the !!. For now, lets not worry about a computation result
    // in the WHERE clause returning something other than a boolean. 
    // Let's assume that the parser takes care of that for us. 
    return !!compute(expr, filteredRow, filtredTable.columnIndexMap, filtredTable.lockedAt);
  }
}