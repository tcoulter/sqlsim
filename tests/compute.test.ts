import compute, { AggregateExpression, AvailableAggregations, BinaryExpression, Expression, LiteralValue, computeAggregates, extractAggregateExpressions, stringifyExpression } from "../compute"
import { ColumnRef } from "../execute";
import Table, { AggregateTable } from "../storage/table";
import { aggregateFunction, columnRef, expression, expressionList } from "./helpers";

describe("Compute", () => {
  function run(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue) {
    let expr = expression(left, operator, right);
    return compute(expr)
  }

  function runTable(left:Expression|LiteralValue, operator: BinaryExpression['operator'], right:Expression|LiteralValue, table:Table) {
    let expr = expression(left, operator, right);
    return table.getRows().map((row) => {
      return compute(expr, row, table.sourceMap());
    });
  }

  function runAggregate(name:AvailableAggregations, expr:ColumnRef|BinaryExpression, table:Table, groupBy?:Array<ColumnRef>) {
    return computeAggregates({
      aggregates:  [aggregateFunction(name, expr)],
      rows: table.getRows(),
      columnIndexMap: table.sourceMap(),
      groupColumns: groupBy
    }).map((row) => row.getData());
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
    let table = new Table({name: "People", columns: ["name", "age"]});
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
    let table = new Table({name: "People", columns: ["name", "age"]});
    table.insert([
      ["Tim", 30],
      ["Liz", 21]
    ]);

    expect(runAggregate("AVG", columnRef("age"), table)).toEqual([
      ["Tim", 30, 25.5],
      ["Liz", 21, 25.5]
    ]);
    expect(runAggregate("SUM", columnRef("age"), table)).toEqual([
      ["Tim", 30, 51],
      ["Liz", 21, 51]
    ]);
    expect(runAggregate("MIN", columnRef("age"), table)).toEqual([
      ["Tim", 30, 21],
      ["Liz", 21, 21]
    ]);
    expect(runAggregate("MAX", columnRef("age"), table)).toEqual([
      ["Tim", 30, 30],
      ["Liz", 21, 30]
    ]);

    // Usually COUNT(*) is used... we don't support the * yet
    expect(runAggregate("COUNT", columnRef("age"), table)).toEqual([
      ["Tim", 30, 2],
      ["Liz", 21, 2]
    ]);

    // With an expression inside the aggregation (e.g., SUM(age + 100) )
    expect(runAggregate("SUM", expression(columnRef("age"), "+", 100), table)).toEqual([
      ["Tim", 30, 251],
      ["Liz", 21, 251]
    ]);

    // With an aggregation inside an expression (e.g., (SUM(age) + 100) )
    // expect(runExpressionWithAggregates(
    //   expression(
    //     aggregateFunction("SUM", columnRef("age")),
    //     "+",
    //     100
    //   ),
    //   table
    // )).toEqual([151]);
  })

  test("aggregate with grouping", () => {
    let table = new Table({name: "People", columns: ["name", "age", "dept"]});
    table.insert([
      ["Tim", 30, "Sales"],
      ["Liz", 21, "Sales"],
      ["Bob", 45, "Accounting"],
      ["Sarah", 35, "Accounting"]
    ]);

    expect(runAggregate("AVG", columnRef("age"), table, [columnRef("dept")])).toEqual([
      ["Tim", 30, "Sales", 25.5],
      ["Liz", 21, "Sales", 25.5],
      ["Bob", 45, "Accounting", 40],
      ["Sarah", 35, "Accounting", 40]
    ]);
  })

  test("IN operator", () => {
    let table = new Table({name: "People", columns: ["name", "age", "dept"]});
    table.insert([
      ["Tim", 30, "Sales"],
      ["Liz", 21, "Sales"],
      ["Bob", 45, "Accounting"],
      ["Sarah", 35, "Accounting"]
    ]);

    expect(runTable(columnRef("age"), "IN", expressionList([30, 35]), table)).toEqual([
      true,
      false,
      false,
      true
    ]);
  })

  test("NOT IN operator", () => {
    let table = new Table({name: "People", columns: ["name", "age", "dept"]});
    table.insert([
      ["Tim", 30, "Sales"],
      ["Liz", 21, "Sales"],
      ["Bob", 45, "Accounting"],
      ["Sarah", 35, "Accounting"]
    ]);

    expect(runTable(columnRef("age"), "NOT IN", expressionList([30, 35]), table)).toEqual([
      false,
      true,
      true,
      false
    ]);
  })

  test("IS operator", () => {
    let table = new Table({name: "People", columns: ["name", "age", "is_hired"]});
    table.insert([
      ["Tim", 30, true],
      ["Liz", 21, false],
      ["Bob", 45, false],
      ["Sarah", 35, true]
    ]);

    expect(runTable(columnRef("is_hired"), "IS", true, table)).toEqual([
      true,
      false,
      false,
      true
    ]);

    expect(runTable(columnRef("name"), "IS", true, table)).toEqual([
      false,
      false,
      false,
      false
    ]);

    expect(runTable(columnRef("age"), "IS", true, table)).toEqual([
      true,
      true,
      true,
      true
    ]);
  })

  test("IS NOT operator", () => {
    let table = new Table({name: "People", columns: ["name", "age", "is_hired"]});
    table.insert([
      ["Tim", 30, true],
      ["Liz", 21, false],
      ["Bob", 45, false],
      ["Sarah", 35, true]
    ]);

    expect(runTable(columnRef("is_hired"), "IS NOT", true, table)).toEqual([
      false,
      true,
      true,
      false
    ]);

    expect(runTable(columnRef("name"), "IS NOT", true, table)).toEqual([
      true,
      true,
      true,
      true
    ]);

    expect(runTable(columnRef("age"), "IS NOT", true, table)).toEqual([
      false,
      false,
      false,
      false
    ]);
  })
})