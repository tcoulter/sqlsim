import { BinaryExpression, Expression, Literal, LiteralValue} from "../compute"
import { ColumnRef } from "../execute";

export function expression(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue):BinaryExpression {
  return {
    type: "binary_expr",
    left: typeof left == "object" ? left as Expression: literal(left),
    right: typeof right == "object" ? right as Expression: literal(right),
    operator: operator
  }
}

export function columnRef(name:string):ColumnRef {
  return {
    type: "column_ref",
    table: null,
    column: name
  }
}

export function literal(value:LiteralValue):Literal {
  if (value == null) {
    return {
      type: "null",
      value: null
    }
  }

  switch(typeof value) {
    case "string":
      return {
        type: "single_quote_string",
        value: value
      }
    case "number":
      return {
        type: "number",
        value: value
      }
    case "boolean":
      return {
        type: "bool",
        value: value
      }
    default: 
      throw new Error("Unexpected literal type: " + typeof value);
  }
} 
