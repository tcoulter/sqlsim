import { AggregateExpression, AvailableAggregations, BinaryExpression, Expression, ExpressionList, Literal, LiteralValue} from "../compute"
import { ColumnRef, OrderBy } from "../execute";

export function expression(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue):BinaryExpression {
  return {
    type: "binary_expr",
    left: typeof left == "object" ? left as Expression: literal(left),
    right: typeof right == "object" ? right as Expression: literal(right),
    operator: operator
  }
}

export function expressionList(expressions:Array<BinaryExpression|LiteralValue>):ExpressionList {
  return {
    type: "expr_list",
    value: expressions.map((valueOrExpression) => {
      if (valueOrExpression == null) {
        return literal(valueOrExpression);
      }

      if (typeof valueOrExpression == "object") {
        return valueOrExpression;
      } else {
        return literal(valueOrExpression);
      }
    })
  }
}

export function columnRef(tableOrColumnName:string, columnName?:string):ColumnRef {
  let tableName:string|null = null;

  if (typeof columnName != "undefined") {
    tableName = tableOrColumnName;
  } else {
    columnName = tableOrColumnName;
  }

  return {
    type: "column_ref",
    table: tableName,
    column: columnName as string
  }
}

export function orderByRef(expr:ColumnRef|BinaryExpression|AggregateExpression, type:"ASC"|"DESC" = "ASC"):OrderBy {
  return {
    expr: expr,
    type: type
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

export function aggregateFunction(name:AvailableAggregations, expr:ColumnRef|BinaryExpression):AggregateExpression {
  return {
    type: "aggr_func",
    name: name, 
    args: {
      expr: expr
    },
    over: null
  }
}