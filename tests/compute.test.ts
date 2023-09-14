import compute, { AggregateExpression, AvailableAggregations, BinaryExpression, Expression, LiteralValue, computeAggregates, extractAggregateExpressions, stringifyExpression } from "../compute"
import { ColumnRef } from "../execute";
import Table from "../storage/table";
import { aggregateFunction, columnRef, expression } from "./helpers";

describe("Compute", () => {
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

  function runAggregate(name:AvailableAggregations, expr:ColumnRef|BinaryExpression, table:Table) {
    return computeAggregates([
      aggregateFunction(name, expr)
    ], table.getRows(), table.columnIndexMap)[0].getData();
  }

  function runExpressionWithAggregates(expr:ColumnRef|BinaryExpression|AggregateExpression, table:Table) {
    return computeAggregates([expr], table.getRows(), table.columnIndexMap)[0].getData();
  }

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

  // TODO: Test expression lists

  test("expressions stringify just fine", () => {
    expect(stringifyExpression(expression(columnRef("age"), ">", 25))).toEqual("age>25");
    expect(stringifyExpression(
      expression(
        expression(columnRef("age"), ">", 25),
        "AND",
        expression(columnRef("name"), "=", "Tim")
      )
    )).toEqual("(age>25)AND(name='Tim')");
  })

  test("LIKE operator", () => {
    expect(run('New York City', "LIKE", 'New %')).toBe(true);
    expect(run('Oregon', "LIKE", 'New %')).toBe(false);
    expect(run('Brand New Shoes', "LIKE", '% New %')).toBe(true);
    expect(run('8Ball', "LIKE", '_Ball')).toBe(true);
    expect(run('8Ball', "LIKE", '_')).toBe(false);
    expect(run('8Ball', "LIKE", '%')).toBe(true);
  })

  test("aggregate computation", () => {
    let table = new Table("People", ["name", "age"]);
    table.insert([
      ["Tim", 30],
      ["Liz", 21]
    ]);

    expect(runAggregate("AVG", columnRef("age"), table)).toEqual([25.5]);
    expect(runAggregate("SUM", columnRef("age"), table)).toEqual([51]);
    expect(runAggregate("MIN", columnRef("age"), table)).toEqual([21]);
    expect(runAggregate("MAX", columnRef("age"), table)).toEqual([30]);

    // Usually COUNT(*) is used... we don't support the * yet
    expect(runAggregate("COUNT", columnRef("age"), table)).toEqual([2]);

    // With an expression inside the aggregation (e.g., SUM(age + 100) )
    expect(runAggregate("SUM", expression(columnRef("age"), "+", 100), table)).toEqual([251]);

    // With an aggregation inside an expression (e.g., (SUM(age) + 100) )
    expect(runExpressionWithAggregates(
      expression(
        aggregateFunction("SUM", columnRef("age")),
        "+",
        100
      ),
      table
    )).toEqual([151]);
  })
})