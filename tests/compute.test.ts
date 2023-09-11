import compute, { BinaryExpression, Expression, ExpressionList, Literal, LiteralValue, NumberLiteral, StringLiteral, analyzeExpression } from "../compute"
import { ColumnRef } from "../execute";
import Table from "../storage/table";

describe("Compute", () => {

  function expression(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue):BinaryExpression {
    return {
      type: "binary_expr",
      left: typeof left == "object" ? left as Expression: literal(left),
      right: typeof right == "object" ? right as Expression: literal(right),
      operator: operator
    }
  }

  function run(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue) {
    let expr = expression(left, operator, right);
    return compute(expr)
  }

  function runTable(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue, table:Table) {
    let expr = expression(left, operator, right);
    return table.getRows().map((row) => {
      return compute(expr, row, table.columnIndexMap);
    });
  }

  function columnRef(name:string):ColumnRef {
    return {
      type: "column_ref",
      table: null,
      column: name
    }
  }

  function literal(value:LiteralValue):Literal {
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

  test("analyzer doesn't pull any columns on literals", () => {
    let requiredColumns = analyzeExpression(
      expression(5, "!=", 2)
    );

    expect(requiredColumns).toEqual([]);
  })

  test("analyzer correctly pulls single column (age)", () => {
    let requiredColumns = analyzeExpression(
      expression(columnRef("age"), ">", 25)
    );

    expect(requiredColumns).toEqual(["age"]);
  })

  test("analyzer correctly pulls columns from complex expression list", () => {
    let requiredColumns = analyzeExpression(
      {
        type: "expr_list",
        value: [
          expression(columnRef("age"), ">", 30),
          expression(columnRef("name"), "=", "Tim")
        ]
      } as ExpressionList
    );

    expect(requiredColumns).toEqual(["age", "name"]);
  })

  test("computes simple boolean expressions with literals", () => {
    expect(run(30, ">", 21)).toBe(true);
    expect(run(30, ">=", 30)).toBe(true);
    expect(run(30, "<", 21)).toBe(false);
    expect(run(30, "<=", 21)).toBe(false);
    expect(run(30, "=", 30)).toBe(true);
  });

  test("computes boolean operators (AND, OR, etc.) with nested expressions", () => {
    // I didn't write a test for every permutation of the booleans on either
    // side of the operator. Likely not necessary given previous tests. (Famous last words?)
    expect(
      run(
        expression(30, ">", 21),
        "AND",
        expression(21, "<", 30)
      )
    ).toBe(true);

    expect(
      run(
        expression(30, ">", 21),
        "AND",
        expression(21, ">", 30)
      )
    ).toBe(false);

    expect(
      run(
        expression(30, ">", 21),
        "OR",
        expression(21, ">", 30)
      )
    ).toBe(true);
  })

  test("expressions with column references correctly pull column data", () => {
    let table = new Table("People", ["name", "age"]);
    table.insert([
      ["Tim", 30],
      ["Liz", 21]
    ]);

    expect(runTable(columnRef("age"), ">", 25, table)).toEqual([
      true,
      false
    ])

    expect(runTable(columnRef("name"), "=", "Liz", table)).toEqual([
      false,
      true
    ])
  })

})