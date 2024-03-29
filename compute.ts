import { AggrFunc as ASTAggrFunc } from "node-sql-parser";
import { ColumnRef, Subquery, executeFromAST } from "./execute";
import { CellData } from "./storage/cell";
import { Commit } from "./storage/commit";
import Row, { JoinedRow } from "./storage/row";
import ColumnIndexMap from "./storage/columnindexmap";
import Storage from "./storage";
import { LockedTable } from "./storage/table";

// Damn parser doesn't give us an Expression type

export type Expression = ColumnRef | Literal | BinaryExpression | AggregateExpression | ExpressionList | Subquery;
export type SingleExpression = ColumnRef | Literal | BinaryExpression | Subquery;
export type Literal = NumberLiteral | StringLiteral | NullLiteral | BooleanLiteral;

// TODO: Test different expressions to see how parser responds
export type BinaryExpression = {
  type: "binary_expr",
  operator: "+"|"-"|"/"|"*"|"="|"!="|"<"|"<="|">"|">="|"<>"|"AND"|"OR"|"IS"|"IS NOT"|"IN"|"NOT IN"|"LIKE",
  left: Expression,
  right: Expression
}

export type ExpressionList = {
  type: "expr_list", 
  value: Array<SingleExpression>
};

export type NumberLiteral = {
  type: "number",
  value: number
}

export type StringLiteral = {
  type: "single_quote_string",
  value: string
}

export type NullLiteral = {
  type: "null", 
  value: null
}

export type BooleanLiteral = {
  type: "bool", 
  value: boolean
}

// These two types are the same. They have different names for descriptive
// purposes, when used pre- or post-computation.
export type LiteralValue = Literal['value'];
export type ComputationResult = LiteralValue;

export function stringifyExpression(expr:Expression, depth:number = 0, royalColumnReferences = false):string {
  switch(expr.type) {
    case "column_ref": 
      if (royalColumnReferences == true) {
        return !!expr.table ? expr.table + "." + expr.column : expr.column;
      } else {
        return expr.column;
      }  
    case "binary_expr": 
      let returnVal = stringifyExpression(expr.left, depth + 1, royalColumnReferences) + expr.operator + stringifyExpression(expr.right, depth + 1, royalColumnReferences);
      if (depth > 0) {
        returnVal = "(" + returnVal + ")";
      }
      return returnVal;
    case "expr_list": 
      return expr.value.map((item) => {
        return stringifyExpression(item, depth, royalColumnReferences);
      }).reduce((previousValue, currentValue) => {
        return previousValue + "," + currentValue;
      }, "");
    case "null":
      return "null";
    case "number":
    case "bool":
      return expr.value.toString();
    case "single_quote_string":
      return "'" + expr.value.toString() + "'";
    case "aggr_func": 
      return computeAggregateName(expr);
    case "sub_query":
      return "<subquery>";
    default:
      // Used 'any' because Typescript doesn't think this will ever run. But it's good as a safeguard. 
      throw new Error("FATAL: Unknown expression type " + (expr as any).type);
  }
}

export default function compute(expr:Expression, row:Row = new Row([]), columnIndexMap:ColumnIndexMap, storage:Storage, commit?:Commit):ComputationResult {
  switch(expr.type) {
    case "binary_expr": 
      let left = (expr.left.type == "expr_list" || expr.left.type == "sub_query")
        ? computeExpressionListOrSubquery(expr.left, row, columnIndexMap, storage, commit) 
        : compute(expr.left, row, columnIndexMap, storage, commit);
      let right = (expr.right.type == "expr_list" || expr.right.type == "sub_query")
        ? computeExpressionListOrSubquery(expr.right, row, columnIndexMap, storage, commit) 
        : compute(expr.right, row, columnIndexMap, storage, commit);
      return computeBinaryOperation(left, right, expr.operator);
    case "number":
    case "single_quote_string": 
    case "bool": 
    case "null": 
      return expr.value;
    case "column_ref":
      // If we have a royal column reference (royal, meaning it includes both table and column)
      // see if we can get this data from the stack first. 
      if (expr.table != null && !columnIndexMap.hasColumn({expr, as: null})) {
        let contextData:CellData|undefined = storage.getStackData(expr, commit);

        if (typeof contextData != "undefined") {
          return contextData;
        }
      }

      // If we can't get it from the stack, and our current context doesn't have a record of
      // the table, then error. 
      if (expr.table != null && !columnIndexMap.hasTable(expr.table)) {
        throw new Error("Unknown table " + expr.table);
      }

      let dataIndex = columnIndexMap.getColumnMapping({expr, as: null});

      if (typeof dataIndex == "undefined") {
        throw new Error("Cannot find column '" + stringifyExpression(expr, 0, true) + "'");
      }

      let value:CellData;
      if (typeof dataIndex == "number") {
        value = row.cell(dataIndex).getData(commit);
      } else {
        value = compute(dataIndex, row, columnIndexMap, storage, commit) as LiteralValue;
      }

      //console.log("Resolving column " + JSON.stringify(expr) + " (index: " + dataIndex + ") to value:", value);
      return value;
    case "aggr_func": 
      return row.cell(columnIndexMap.getColumnMapping({
        expr,
        as: null
      }) as number).getData(commit);
    case "expr_list": 
      throw new Error("Unexpected Error: Expression lists should only be allowed in binary expressions (and are handled there)")
    default: 
      // Used 'any' because Typescript doesn't think this will ever run. But it's good as a safeguard. 
      throw new Error("Expression type " + (expr as any).type + " not yet supported");
  }
}

function computeExpressionListOrSubquery(expr:ExpressionList|Subquery, row:Row, columnIndexMap:ColumnIndexMap, storage:Storage, commit?:Commit):Array<ComputationResult> {
  if (expr.type == "sub_query") {
    // Create a new storage object to bump up the stack frame
    let subqueryStorage = Storage.newStackFrame(storage, row, columnIndexMap);
    let subqueryResult = (executeFromAST([expr.ast], subqueryStorage)[0] as LockedTable)
    
    return getFirstColumn(subqueryResult.getData());
  } else {
    return expr.value.map((value) => {
      if (value.type == "sub_query") {
        return computeExpressionListOrSubquery(value, row, columnIndexMap, storage, commit);
      } else {
        return compute(value, row, columnIndexMap, storage, commit) as LiteralValue;
      }
    }).flat()
  }
}

function computeBinaryOperation(left:ComputationResult|Array<ComputationResult>, right:ComputationResult|Array<ComputationResult>, operator:BinaryExpression['operator']):ComputationResult {
  // Get computation results in the right format first
  switch(operator) {
    case "+":
    case "-":
    case "/":
    case "*":
    case "=":
    case "!=":
    case "<":
    case "<=":
    case ">":
    case ">=":
    case "<>":
    case "AND":
    case "OR":
    case "IS":
    case "IS NOT":
    case "LIKE":
      left = ensureFirstValue(left, operator);
      right = ensureFirstValue(right, operator);
      break;
    case "IN":
    case "NOT IN":
      break;
  }
  
  switch(operator) {
    case "+":
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) + (right as number);
    case "-":
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) - (right as number);
    case "/":
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) / (right as number);
    case "*":
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) * (right as number);
    case "=":
      return left == right;
    
    case "<>":
    case "!=":
      return left != right;
    case "<":
      // TODO: Check if we can use this operator on other types
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) < (right as number);
    case "<=":
      // TODO: Check if we can use this operator on other types
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) <= (right as number);
    case ">":
      // TODO: Check if we can use this operator on other types
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) > (right as number);
    case ">=":
      // TODO: Check if we can use this operator on other types
      enforceNumber(left);
      enforceNumber(right);
      return (left as number) >= (right as number);
    case "AND":
      enforceBoolean(left);
      enforceBoolean(right);
      return (left as boolean) && (right as boolean);
    case "OR":
      enforceBoolean(left);
      enforceBoolean(right);
      return (left as boolean) || (right as boolean);
    case "LIKE":
      enforceString(left); 
      enforceString(right);
      let regex = convertLIKEPatternToRegex(right as string);
      return regex.test(left as string);
    case "IN":
      enforceArray(right);
      right = right as Array<ComputationResult>;
      let found = false;
      for (var i = 0; i < right.length; i++) {
        // double bang to enforce a boolean
        found = !!computeBinaryOperation(left, right[i], "=");
        if (found == true) {
          break;
        }
      }
      return found;
    case "NOT IN":
      return computeBinaryOperation(left, right, "IN") == false;
    case "IS": 
      enforceBoolean(right);
      let truthiness:boolean = false;

      // Mimic truthiness allowed by MySQL
      if (typeof left == "boolean") {
        truthiness = left;
      } else if (typeof left == "number") {
        truthiness = left != 0; 
      }

      return truthiness;
    case "IS NOT":
      return computeBinaryOperation(left, right, "IS") == false;
    default: 
      throw new Error("Unexpected expression operator " + operator + "; operator not yet supported");
  }
}

function enforceNumber(value:ComputationResult|Array<ComputationResult>) {
  if (typeof value != "number") {
    throw new Error("Expected number type but got " + typeof value);
  }
}

function enforceBoolean(value:ComputationResult|Array<ComputationResult>) {
  if (typeof value != "boolean") {
    throw new Error("Expected boolean type but got " + typeof value);
  }
}

function enforceString(value:ComputationResult|Array<ComputationResult>) {
  if (typeof value != "string") {
    throw new Error("Expected string type but got " + typeof value);
  }
}

function enforceArray(value:ComputationResult|Array<ComputationResult>) {
  if (Array.isArray(value) == false) {
    throw new Error("Expected list/tuple but got " + typeof value);
  }
}

function ensureFirstValue(value:ComputationResult|Array<ComputationResult>, operator:BinaryExpression['operator']):ComputationResult {
  if (Array.isArray(value)) {
    if (value.length == 1) {
      return value[0];
    } else {
      throw new Error("Unexpected number of parameters/columns given to operator '" + operator + "'")
    }
  }

  return value;
}

function getFirstColumn(data:CellData[][]) {
  return data.map((rowData:CellData[]) => rowData[0]);
}

function convertLIKEPatternToRegex(pattern:string):RegExp {
  let regexPattern = "^" + pattern.replace(/%/g, ".*?").replace(/_/g, ".") + "$";
  return new RegExp(regexPattern);
}

// This exists literally because I don't want to keep telling typescript
// the type of certain data that's stored. So let's put often-used types
// in buckets. 
type AggregatorData = {
  numbers:Record<string, number>,
  strings:Record<string, string>,
  booleans:Record<string, boolean>
}

type AggregatorStartFn = (data:AggregatorData) => void;
type AggregatorNextFn = (value:CellData, data:AggregatorData) => void;
type AggregatorResultFn = (data:AggregatorData) => CellData;

class Aggregator {
  name:string;
  expr:AggregateExpression;
  data:AggregatorData;
  #next:AggregatorNextFn;
  #result:AggregatorResultFn;
  allowsNull:boolean;

  constructor(expr:AggregateExpression, {start, next, result}:AggregatorDefinition, allowsNull = false) {
    this.name = computeAggregateName(expr);
    this.expr = expr;
    this.#next = next;
    this.#result = result;
    this.allowsNull = allowsNull;

    this.data = {numbers: {}, strings: {}, booleans: {}};

    start(this.data);
  }

  next(value:CellData) {
    // Ignore null values
    if (!this.allowsNull && value == null) {
      return;
    }

    this.#next(value, this.data);
  }

  result():CellData {
    return this.#result(this.data);
  }
}

type AggregatorDefinition = {
  start:AggregatorStartFn,
  next:AggregatorNextFn,
  result:AggregatorResultFn
};
export type AvailableAggregations = "AVG" | "SUM" | "MIN" | "MAX" | "COUNT";
type AggregatorDefinitionList = Record<AvailableAggregations, AggregatorDefinition>;

// TODO: Enforce type checking somewhere so we don't end up with strings
// being passed to AVG, etc.
export const aggregatorDefinitions:AggregatorDefinitionList = {
  "AVG": {
    start: ({numbers}) => {
      numbers.sum = 0;
      numbers.count = 0;
    },
    next: (value, {numbers}) => {
      numbers.sum   = numbers.sum + (value as number);
      numbers.count = numbers.count + 1;
    }, 
    result: ({numbers}) => {
      // See spec: AVG returns null on no rows
      return numbers.count == 0 ? null : numbers.sum / numbers.count 
    }
  },
  "SUM": {
    start: ({numbers}) => {
      numbers.sum = 0;
    },
    next: (value, {numbers}) => {
      numbers.sum   = numbers.sum + (value as number);
    }, 
    result: ({numbers}) => {
      return numbers.sum;
    }
  },
  "MIN": {
    start: ({numbers, booleans}) => {
      booleans.foundMin = false;
      numbers.min = Number.MAX_SAFE_INTEGER;
    },
    next: (value, {numbers, booleans}) => {
      if ((value as number) < numbers.min) {
        numbers.min = value as number;
        booleans.foundMin = true;
      }
    }, 
    result: ({numbers, booleans}) => {
      // Spec says to return null if no minimum is found
      return booleans.foundMin ? numbers.min : null;
    }
  },
  "MAX": {
    start: ({numbers, booleans}) => {
      booleans.foundMax = false;
      numbers.max = Number.MIN_SAFE_INTEGER;
    },
    next: (value, {numbers, booleans}) => {
      if ((value as number) > numbers.max) {
        numbers.max = value as number;
        booleans.foundMax = true;
      }
    }, 
    result: ({numbers, booleans}) => {
      // Spec says to return null if no maximum is found
      return booleans.foundMax ? numbers.max : null;
    }
  },
  "COUNT": {
    start: ({numbers}) => {
      numbers.count = 0;
    },
    next: (value, {numbers}) => {
      numbers.count += 1;
    }, 
    result: ({numbers}) => {
      return numbers.count;
    }
  },
};

// Apparently the type we get from the AST doesn't match the JSON data
// e.g., Where does the expr come from???
export type AggregateExpression = Omit<ASTAggrFunc, 'args'> & {
  type: "aggr_func",
  args: {
    expr: ColumnRef | BinaryExpression // TODO: Support full expressions, *, and inner functions
  },
  over: null
}

export function computeAggregateName(expr:AggregateExpression):string {
  let name = expr.name + "(";
  name += stringifyExpression(expr.args.expr);
  name += ")";
  return name;
}

export type ComputeAggregatesOptions = {
  aggregates: Array<AggregateExpression>,
  rows:Array<Row>,
  columnIndexMap:ColumnIndexMap,
  storage:Storage,
  groupColumns?:Array<ColumnRef>,
  commit?:Commit
}

export function computeAggregates({aggregates, rows, columnIndexMap, storage, groupColumns = [], commit}:ComputeAggregatesOptions):Array<Row> {
  type AggregatorByHash = Record<string, {
    groupCombination: CellData[],
    aggregators: Array<Aggregator>
  }>;

  let aggregatorsByCombination:AggregatorByHash = {};

  rows.forEach((row) => {
    let groupCombination = groupColumns.map((column) => {
      // TODO: Can this point to a binary expression? 
      return row.cell(columnIndexMap.getColumnMapping({
        expr: column,
        as: null
      }) as number).getData(commit)
    })

    // Use JSON.stringify() as our hashing function. Is is slow? Probably.
    // Do we care right now? Not really. 
    let hash = JSON.stringify(groupCombination);

    if (typeof aggregatorsByCombination[hash] == "undefined") {
      aggregatorsByCombination[hash] = {
        groupCombination,
        aggregators: aggregates.map((expr) => {
          return new Aggregator(expr, aggregatorDefinitions[expr.name as AvailableAggregations])
        })
      }
    }

    aggregatorsByCombination[hash].aggregators.forEach((aggregator) => {
      // Note that we're computing what's inside the aggregation here. 
      let value:CellData = compute(aggregator.expr.args.expr, row, columnIndexMap, storage, commit);
      aggregator.next(value);
    })
  });

  let aggregatorResultsByCombination:Record<string, Row> = {};

  Object.entries(aggregatorsByCombination).forEach(([hash, {aggregators}]) => {
    aggregatorResultsByCombination[hash] = new Row(aggregators.map((aggregator) => aggregator.result()), commit)
  });

  return rows.map((sourceRow) => {
    let groupCombination = groupColumns.map((column) => {
      // TODO: Can this point to a binary expression? 
      return sourceRow.cell(columnIndexMap.getColumnMapping({
        expr: column,
        as: null
      }) as number).getData(commit)
    })

    let hash = JSON.stringify(groupCombination);

    return new JoinedRow(
      sourceRow,
      aggregatorResultsByCombination[hash]
    );
  })
}


export function includesAggregation(expr:Expression):boolean {
  switch(expr.type) {
    case "binary_expr":
      return includesAggregation(expr.left) || includesAggregation(expr.right);
    case "aggr_func":
      return true;
    case "number":
    case "column_ref":
    case "single_quote_string":
    case "null":
    case "bool":
      return false;
    default: 
      throw new Error("Unexpected expression type: " + expr.type);
  }
}

export function extractAggregateExpressions(expr:Expression):Array<AggregateExpression> {
  switch(expr.type) {
    case "binary_expr":
      return extractAggregateExpressions(expr.left).concat(extractAggregateExpressions(expr.right));
    case "aggr_func":
      return [expr];
    case "number":
    case "column_ref":
    case "single_quote_string":
    case "null":
    case "bool":
      return [];
    default: 
      throw new Error("Unexpected expression type: " + expr.type);
  }
}

export function setTableNamePrefixesWhereNull(tableName:string, expr:Expression) {
  switch(expr.type) {
    case "binary_expr":
      setTableNamePrefixesWhereNull(tableName, expr.left);
      setTableNamePrefixesWhereNull(tableName, expr.right);
      break;
    case "aggr_func":
      setTableNamePrefixesWhereNull(tableName, (expr as AggregateExpression).args.expr);
    case "column_ref":
      let columnRef:ColumnRef = expr as ColumnRef;
      if (columnRef.table == null) {
        columnRef.table = tableName;
      }
    case "number":
    case "single_quote_string":
    case "null":
    case "bool":
      break;
    default: 
      throw new Error("Unexpected expression type: " + expr.type);
  }
}

