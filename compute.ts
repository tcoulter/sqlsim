import { AggrFunc as ASTAggrFunc } from "node-sql-parser";
import { ColumnRef } from "./execute";
import { CellData } from "./storage/cell";
import { Commit } from "./storage/commit";
import Row from "./storage/row";
import { ColumnIndexMap } from "./storage/table";

// Damn parser doesn't give us an Expression type

export type Expression = ColumnRef | Literal | BinaryExpression | ExpressionList;
export type SingleExpression = ColumnRef | Literal | BinaryExpression;
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
  value: Array<BinaryExpression|Literal>
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

export function stringifyExpression(expr:ColumnRef|Literal|BinaryExpression|ExpressionList, depth:number = 0):string {
  switch(expr.type) {
    case "column_ref": 
      return expr.column;
    case "binary_expr": 
      let returnVal = stringifyExpression(expr.left, depth + 1) + expr.operator + stringifyExpression(expr.right, depth + 1);
      if (depth > 0) {
        returnVal = "(" + returnVal + ")";
      }
      return returnVal;
    case "expr_list": 
      return expr.value.map((item) => {
        return stringifyExpression(item);
      }).reduce((previousValue, currentValue) => {
        return previousValue + "," + currentValue;
      }, "");
    case "null":
      return "null";
    case "number":
      return expr.value.toString();
    case "single_quote_string":
      return "'" + expr.value.toString() + "'";
    default:
      // I want to have this error here for safety, but Typescript doesn't think we'll ever
      // run this line (neither do I). Added an 'any' to make Typescript happy and ensure
      // we're better safe than sorry. 
      throw new Error("FATAL: Unknown expression type " + (expr as any).type);
  }
}

export default function compute(expr:ColumnRef|Literal|BinaryExpression|ExpressionList, row:Row = new Row([]), columnIndexMap:ColumnIndexMap = {}, commit?:Commit):ComputationResult {
  switch(expr.type) {
    case "binary_expr": 
      let left = compute(expr.left, row, columnIndexMap);
      let right = compute(expr.right, row, columnIndexMap);
      return computeBinaryOperation(left, right, expr.operator);
    case "number":
    case "single_quote_string": 
    case "bool": 
    case "null": 
      return expr.value;
    case "column_ref": 
      // Here, column ref will always references a named column, hence the 'as number'
      // TODO: Test this! 
      let dataIndex:number = columnIndexMap[expr.column] as number;
      if (typeof dataIndex == "undefined") {
        console.log(columnIndexMap);
        throw new Error("Cannot find column " + expr.column);
      }
      let value = row.cell(dataIndex).getData(commit);
      // console.log("Resolving column " + expr.column + " (index: " + dataIndex + ") to value:", value);
      return value;
    default: 
      throw new Error("Expression type " + expr.type + " not yet supported");
  }
}

function computeBinaryOperation(left:ComputationResult, right:ComputationResult, operator:BinaryExpression['operator']):ComputationResult {
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
    case "IS": // TODO: Need to double check that this can only be used on nulls
    case "=":
      return left == right;
    case "IS NOT": // TODO: Need to double check that this can only be used on nulls
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
    case "NOT IN":
      throw new Error("IN / NOT IN not yet supported");
    default: 
      throw new Error("Unexpected expression operator " + operator + "; operator not yet supported");
  }
}

function enforceNumber(value:ComputationResult) {
  if (typeof value != "number") {
    throw new Error("Expected number type but got " + typeof value);
  }
}

function enforceBoolean(value:ComputationResult) {
  if (typeof value != "boolean") {
    throw new Error("Expected boolean type but got " + typeof value);
  }
}

function enforceString(value:ComputationResult) {
  if (typeof value != "string") {
    throw new Error("Expected string type but got " + typeof value);
  }
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
  data:AggregatorData;
  #next:AggregatorNextFn;
  #result:AggregatorResultFn;
  allowsNull:boolean;

  constructor(name:string, {start, next, result}:AggregatorDefinition, allowsNull = false) {
    this.name = name;
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
    expr: ColumnRef // TODO: Support full expressions, *, and inner functions
  },
  over: null
}

export function computeAggregateName(expr:AggregateExpression):string {
  return expr.name + "(" + expr.args.expr.column + ")";
}

// TODO: Group by non-aggregate functions passed (all column refs)
export function computeAggregates(columns:Array<ColumnRef|AggregateExpression>, rows:Array<Row>, columnIndexMap:ColumnIndexMap, commit?:Commit):Array<Array<CellData>> {
  // This maps directly to columns, replacing Aggregate expressions with aggregators
  let columnsWithAggregators:Array<ColumnRef|Aggregator> = [];

  // These two are meant to have the same length, indexes mapping to related data
  let aggregateExpressions:Array<AggregateExpression> = [];
  let aggregators:Array<Aggregator> = [];

  columns.forEach((column) => {
    if (column.type == "aggr_func") {
      let name = computeAggregateName(column);
      let aggregator = new Aggregator(name, aggregatorDefinitions[column.name as AvailableAggregations]);

      aggregators.push(aggregator);
      aggregateExpressions.push(column);

      columnsWithAggregators.push(aggregator);
    } else {
      columnsWithAggregators.push(column);
    }
  });

  rows.forEach((row) => {
    aggregateExpressions.forEach((aggrFunc, funcIndex) => {
      let column = aggrFunc.args.expr.column;
      let dataIndex = columnIndexMap[column]; 

      // TODO: Do we need this error? Could it actually be fine? Should we just
      // run the expression first? 
      if (typeof dataIndex != "number") {
        throw new Error("Unexpected expression source for column " + column + " during aggregation");
      }

      // Throw the next value through the aggregator
      aggregators[funcIndex].next(row.cell(dataIndex as number).getData(commit))
    })
  });

  // TODO: Null out any non-aggregate columns for now. 
  return [
    columnsWithAggregators.map((column) => {
      if (column instanceof Aggregator) {
        return column.result();
      } else {
        return null;
      }
    })
  ];
}