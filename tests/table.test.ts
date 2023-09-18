import Cell, { CellData } from "../storage/cell";
import Row from "../storage/row";
import Table, { DistinctTable, FilteredTable, JoinedTable, LockedTable, OrderedTable, ProjectedTable } from "../storage/table";
import { columnRef, expression, orderByRef } from "./helpers";

describe("LockedTable", () => {
  test("cells return data at lock time even after updates", () => {
    let expectedData:Array<CellData> = ["Tim", 30];
    let newData:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(expectedData)
   
    // We're going to do direct updates to the row rather than 
    // through any update mechanism (doesn't exist yet as of this writing).

    let lockedTable = new LockedTable(table);

    // No update yet, so we have to get the row itself
    let rows:Array<Row> = table.getRows();
    rows[0].put(newData);

    expect(lockedTable.getData()).toEqual([expectedData]);
    expect(table.getData()).toEqual([newData]);
  })

  test("returns only rows available at lock time", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);

    let lockedTable = new LockedTable(table);

    table.insert(secondRow);

    expect(lockedTable.getData()).toEqual([
      firstRow
    ]);
    expect(table.getData()).toEqual([
      firstRow,
      secondRow
    ])
  })

  test("return rows available at lock time that were later deleted", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);
    table.insert(secondRow);

    let lockedTable = new LockedTable(table);

    table.getRows()[1].delete();

    expect(lockedTable.getData()).toEqual([
      firstRow,
      secondRow
    ]);
    expect(table.getData()).toEqual([
      firstRow
    ])
  })
})

describe("Table", () => {

  test("rows inserted at the same time all have the same commit", () => {
    let firstRowData:Array<CellData> = ["Tim", 30];
    let secondRowData:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert([
      firstRowData,
      secondRowData
    ]);

    let rows = table.getRows();

    expect(rows.length).toBe(2);
    expect(rows[0].createdAt).toEqual(rows[1].createdAt);
  })

  test("can update values with literals", () => {
    let initialData:Array<Array<CellData>> = [
      ["Tim", 30],
      ["Liz", 21],
      ["Gary", 49],
      ["Preeti", 25]
    ]

    let table = new Table("People", ["name", "age"]);
    table.insert(initialData);

    table.update(['age'], [100], expression(columnRef('age'), ">=", 25));

    expect(table.getData()).toEqual([
      ["Tim", 100],
      ["Liz", 21],
      ["Gary", 100],
      ["Preeti", 100]
    ])
  })

  test("can update values with expressions", () => {
    let initialData:Array<Array<CellData>> = [
      ["Tim", 30],
      ["Liz", 21],
      ["Gary", 49],
      ["Preeti", 25]
    ]

    let table = new Table("People", ["name", "age"]);
    table.insert(initialData);

    table.update(['age'], [expression(columnRef('age'), "+", 5)], expression(columnRef('age'), ">=", 25));

    expect(table.getData()).toEqual([
      ["Tim", 35],
      ["Liz", 21],
      ["Gary", 54],
      ["Preeti", 30]
    ])
  })

  test("can update multiple values, out of order, with/without expressions", () => {
    let initialData:Array<Array<CellData>> = [
      ["Tim", 30],
      ["Liz", 21],
      ["Gary", 49],
      ["Preeti", 25]
    ]

    let table = new Table("People", ["name", "age"]);
    table.insert(initialData);

    // Notice separate order of age and name
    table.update(['age', 'name'], [
      expression(columnRef('age'), "+", 5),
      'Default Human'
    ], expression(columnRef('age'), ">=", 25));

    expect(table.getData()).toEqual([
      ["Default Human", 35],
      ["Liz", 21],
      ["Default Human", 54],
      ["Default Human", 30]
    ])
  })

  test("can filter columns via projection", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);
    table.insert(secondRow);

    let projectedTable = table.project(['name']);

    expect(projectedTable.getData()).toEqual([
      [ firstRow[0]  ],
      [ secondRow[0] ]
    ]);
  })

  test("can reorder columns via projection", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);
    table.insert(secondRow);

    let projectedTable = table.project(['age', 'name']);

    expect(projectedTable.getData()).toEqual([
      [ firstRow[1],  firstRow[0]  ],
      [ secondRow[1], secondRow[0] ]
    ]);
  })

  test("can expand(?) columns via projection", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);
    table.insert(secondRow);

    let projectedTable = table.project(['name', 'name', 'name']);

    expect(projectedTable.getData()).toEqual([
      [ firstRow[0],  firstRow[0],  firstRow[0]  ],
      [ secondRow[0], secondRow[0], secondRow[0] ]
    ]);
  });

  test("can rename columns via projection", () => {
    // Note we're using the Column object here instead of simple names. 
    let table = new Table("People", ["name", "age"]);
    table.insert(["Tim", 30]);

    let firstProjection = new ProjectedTable({
      table, 
      columns: [{
        expr: expression(columnRef("age"), "+", 10),
        as: "older_age"
      }]
    });

    // Do the second projection using "older_age" within an expression,
    // so we can be absolutely sure the AS projected correctly from the
    // first projection. 
    let secondProjection = new ProjectedTable({
      table: firstProjection,
      columns: [{
        expr: expression(columnRef("older_age"), "+", 10),
        as: "even_older_age"
      }]
    });

    expect(secondProjection.getData()).toEqual([
      [50]
    ])
  })
})

describe("JoinedTable", () => {
  test("handles left/right/inner/full joins", () => {
    let people = new Table("People", ["name", "age", "from_id"]);
    let countries = new Table("Countries", ["country_id", "country_name"]);
    
    people.insert([
      ["Tim", 30, 1],
      ["Liz", 21, 2],
      ["Russ", 47, null]  // Note Russ has no from_id
    ]);

    countries.insert([
      [1, "USA"],
      [2, "Canada"],
      [3, "Mexico"]
    ]);

    let innerJoin = new JoinedTable({
      type: "inner",
      left: people, 
      right: countries,
      on: expression(columnRef("from_id"), "=", columnRef("country_id"))
    });

    expect(innerJoin.columns.length).toBe(5);
    expect(innerJoin.columns).toEqual(people.columns.concat(countries.columns));
    expect(innerJoin.getData()).toEqual([
      ["Tim", 30, 1, 1, "USA"],
      ["Liz", 21, 2, 2, "Canada"]
    ])

    let leftJoin = new JoinedTable({
      type: "left",
      left: people, 
      right: countries,
      on: expression(columnRef("from_id"), "=", columnRef("country_id"))
    })

    expect(leftJoin.getData()).toEqual([
      ["Tim", 30, 1, 1, "USA"],
      ["Liz", 21, 2, 2, "Canada"],
      ["Russ", 47, null, null, null]
    ])

    let rightJoin = new JoinedTable({
      type: "right",
      left: people, 
      right: countries,
      on: expression(columnRef("from_id"), "=", columnRef("country_id"))
    })

    expect(rightJoin.getData()).toEqual([
      ["Tim", 30, 1, 1, "USA"],
      ["Liz", 21, 2, 2, "Canada"],
      [null, null, null, 3, "Mexico"]
    ]);

    let fullJoin = new JoinedTable({
      type: "full",
      left: people, 
      right: countries,
      on: expression(columnRef("from_id"), "=", columnRef("country_id"))
    })

    expect(fullJoin.getData()).toEqual([
      ["Tim", 30, 1, 1, "USA"],
      ["Liz", 21, 2, 2, "Canada"],
      ["Russ", 47, null, null, null],
      [null, null, null, 3, "Mexico"]
    ])
  })

  test("handles cross joins", () => {
    let tableA = new Table("A", ["one", "two"]);
    let tableB = new Table("B", ["three", "four"]);

    tableA.insert([
      [1, 1],
      [2, 2]
    ]);

    tableB.insert([
      [3, 3],
      [4, 4]
    ])

    let joinedTable = new JoinedTable({
      type: "cross", 
      left: tableA, 
      right: tableB
    });

    expect(joinedTable.getData()).toEqual([
      [1, 1, 3, 3],
      [1, 1, 4, 4],
      [2, 2, 3, 3],
      [2, 2, 4, 4]
    ]);
  })

  test("work the same when locked in computed tables", () => {
    // A computed table is the final object type used before directly computing data
    // All select statements, even with joins, will eventually result in a computed table. 

    let people = new Table("People", ["name", "age", "from_id"]);
    let countries = new Table("Countries", ["country_id", "country_name"]);
    
    people.insert([
      ["Tim", 30, 1],
      ["Liz", 21, 2],
      ["Russ", 47, null]  // Note Russ has no from_id
    ]);

    countries.insert([
      [1, "USA"],
      [2, "Canada"],
      [3, "Mexico"]
    ]);

    let innerJoin = new JoinedTable({
      type: "inner",
      left: people, 
      right: countries,
      on: expression(columnRef("from_id"), "=", columnRef("country_id"))
    });

    let computed = new FilteredTable({
      table: innerJoin
    });

    expect(computed.columns.length).toBe(5);
    expect(computed.columns).toEqual(people.columns.concat(countries.columns));
    expect(computed.getData()).toEqual([
      ["Tim", 30, 1, 1, "USA"],
      ["Liz", 21, 2, 2, "Canada"]
    ])
  })
  // TODO: Make sure joined tables respect row commits
  
  test("maintain column ambiguity (columns with the same name)", () => {
    let a = new Table("A", ["name"]);
    let b = new Table("B", ["name"]);

    a.insert([["Tim"], ["Liz"]]);
    b.insert([["Gary"], ["Russ"]]);

    let joined = new JoinedTable({
      type: "cross",
      left: a,
      right: b
    })

    // We need a projected table so we can select "name" and force the ambiguity
    let projected = new ProjectedTable({
      table: joined,
      columns: [{
        expr: columnRef("name"),
        as: null
      }]
    })

    try {
      projected.getData();
      throw new Error("Test failed; no error was thrown from getData()")
    } catch (e) {
      expect(e.toString()).toEqual("Error: Column 'name' is ambiguous.")
    }
  })

  test("maintain column *dis*ambiguity via table name prefixes", () => {
    let a = new Table("A", ["name"]);
    let b = new Table("B", ["name"]);

    a.insert([["Tim"], ["Liz"]]);
    b.insert([["Gary"], ["Russ"]]);

    let joined = new JoinedTable({
      type: "cross",
      left: a,
      right: b
    })

    // We need a projected table so we can select "A.name" and "B.name" 
    // to route around the ambiguity.
    let projected = new ProjectedTable({
      table: joined,
      columns: [{
        expr: columnRef("A", "name"),
        as: null
      }, {
        expr: columnRef("B", "name"),
        as: null
      }]
    })

    expect(projected.getData()).toEqual([
      ["Tim", "Gary"],
      ["Tim", "Russ"],
      ["Liz", "Gary"],
      ["Liz", "Russ"]
    ])
  })
})

describe("Distinct Table", () => {
  test("it pulls the first distinct of every combination", () => {
    let table = new Table("Table", ["col1", "col2", "col3"]);

    table.insert([
      [1, 1, 1], // Distinct
      [1, 1, 2],
      [1, 2, 1], // Distinct
      [1, 2, 2],
      [2, 1, 1], // Distinct
      [2, 1, 2],
      [2, 2, 1], // Distinct
      [2, 2, 2]
    ]);

    let distinct = new DistinctTable(table, [
      columnRef("col1"),
      columnRef("col2")
    ]);

    expect(distinct.getData()).toEqual([
      [1, 1, 1], // Distinct
      [1, 2, 1], // Distinct
      [2, 1, 1], // Distinct
      [2, 2, 1], // Distinct
    ])
  });

  test("distinct without any grouping", () => {
    // This represents an original table with an aggregate calculation added
    let table = new Table("People", ["name", "age", "AVG(age)"]);

    table.insert([
      ["Tim", 30, 34],
      ["Liz", 21, 34],
      ["Russ", 51, 34]
    ]);

    let distinct = new DistinctTable(table);

    expect(distinct.getData()).toEqual([
      ["Tim", 30, 34]
    ])
  })
})

describe("Ordered Table", () => {
  let table = new Table("People", ["name", "age", "department"]);

  table.insert([
    ["Russ", 51, "Accounting"],
    ["Gary", 49, "Accounting"],
    ["Tim", 30, "Sales"],
    ["Liz", 21, "Sales"]
  ]);

  test("it returns sorted data by one column", () => {
    let sorted = new OrderedTable(table, [
      orderByRef(columnRef("name"))
    ])

    expect(sorted.getData()).toEqual([
      ["Gary", 49, "Accounting"],
      ["Liz", 21, "Sales"],
      ["Russ", 51, "Accounting"],
      ["Tim", 30, "Sales"]
    ])
  })

  test("it returns sorted data by multiple columns", () => {
    let sorted = new OrderedTable(table, [
      orderByRef(columnRef("department")), 
      orderByRef(columnRef("age"))
    ])

    expect(sorted.getData()).toEqual([
      ["Gary", 49, "Accounting"],
      ["Russ", 51, "Accounting"],
      ["Liz", 21, "Sales"],
      ["Tim", 30, "Sales"]
    ])
  })

  test("it returns sorted data by multiple columns in descending order", () => {
    let sorted = new OrderedTable(table, [
      orderByRef(columnRef("department"), "DESC"),
      orderByRef(columnRef("age"), "DESC")
    ])

    expect(sorted.getData()).toEqual([
      ["Tim", 30, "Sales"],
      ["Liz", 21, "Sales"],
      ["Russ", 51, "Accounting"],
      ["Gary", 49, "Accounting"]
    ])
  })

  test("it returns sorted data with expressions ", () => {
    // This should sort first by putting rows in two buckets: The first sort expression
    // returns a boolean, so sorted rows should be separated by that result.
    // Then, it'll sort by name within those buckets, the second in descending order. 
    let sorted = new OrderedTable(table, [
      orderByRef(expression(columnRef("age"), ">", 50)), 
      orderByRef(columnRef("name"), "ASC")
    ])

    expect(sorted.getData()).toEqual([
      ["Gary", 49, "Accounting"], // age <= 50
      ["Liz", 21, "Sales"],
      ["Tim", 30, "Sales"],
      ["Russ", 51, "Accounting"], // age > 50
    ])
  })
})

