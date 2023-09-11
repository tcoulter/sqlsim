import { ColumnRef } from "./execute";
import { CellData } from "./storage/cell";
import { Commit } from "./storage/commit";
import Row from "./storage/row";
import Table, { ColumnIndexMap } from "./storage/table";

// Damn parser doesn't give us an Expression type

export type Expression = ColumnRef | Literal | BinaryExpression | ExpressionList;
export type Literal = NumberLiteral | StringLiteral | NullLiteral | BooleanLiteral;

// TODO: Test different expressions to see how parser responds
export type BinaryExpression = {
  type: "binary_expr",
  operator: "+"|"-"|"/"|"*"|"="|"!="|"<"|"<="|">"|">="|"<>"|"AND"|"OR"|"IS"|"IS NOT"|"IN"|"NOT IN",
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

export function analyzeExpression(expr:ColumnRef|Literal|BinaryExpression|ExpressionList):Array<string> {
  switch(expr.type) {
    case "column_ref": 
      return [expr.column]
    case "binary_expr": 
      return analyzeExpression(expr.left).concat(analyzeExpression(expr.right));
    case "expr_list": 
      return expr.value.map((item) => {
        return analyzeExpression(item);
      }).reduce((previousValue, currentValue) => {
        return previousValue.concat(currentValue);
      }, []);
    case "null":
    case "number":
    case "single_quote_string":
      return [];
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
      let dataIndex = columnIndexMap[expr.column];
      if (typeof dataIndex == "undefined") {
        throw new Error("Cannot find column " + expr.column);
      }
      return row.cells[dataIndex].getData(commit);
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