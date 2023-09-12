import { CellData } from "../storage/cell";
import Row from "../storage/row";
import Table, { FilteredTable, JoinedTable, LockedTable } from "../storage/table";
import { columnRef, expression } from "./helpers";

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
})

describe("JoinedTable", () => {
  test("handles simple joins", () => {
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
})