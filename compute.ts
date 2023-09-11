import { ColumnRef } from "./execute";
import { CellData } from "./storage/cell";

// Damn parser doesn't give us an Expression type

type Expression = BinaryExpression;
type Literal = NumberLiteral | StringLiteral | NullLiteral;

// TODO: Test different expressions to see how parser responds
type BinaryExpression = {
  type: "binary_expr",
  operator: "+"|"-"|"/"|"*"|"="|"!="|"<"|"<="|">"|">="|"<>"|"AND"|"OR"|"IS NOT"|"IN"|"NOT IN",
  left: ColumnRef | Literal | BinaryExpression | ExpressionList,
  right: ColumnRef | Literal | BinaryExpression | ExpressionList
}

type ExpressionList = {
  type: "expr_list", 
  value: Array<BinaryExpression|Literal>
};

type NumberLiteral = {
  type: "number",
  value: number
}

type StringLiteral = {
  type: "single_quote_string",
  value: string
}

type NullLiteral = {
  type: "null", 
  value: null
}

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

export default function compute(expr:BinaryExpression) {
  let neededColumns = analyzeExpression(expr);
}