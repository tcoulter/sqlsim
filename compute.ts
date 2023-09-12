import { ColumnRef } from "./execute";
import { CellData } from "./storage/cell";
import { Commit } from "./storage/commit";
import Row, { JoinedRow } from "./storage/row";
import Table, { ColumnIndexMap } from "./storage/table";

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