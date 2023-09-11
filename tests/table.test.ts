import { CellData } from "../storage/cell";
import Row from "../storage/row";
import Table, { ComputedTable } from "../storage/table";

describe("ComputedTable", () => {
  test("cells return data at lock time even after updates", () => {
    let expectedData:Array<CellData> = ["Tim", 30];
    let newData:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    let row = new Row(expectedData);

    table.rows.push(row);

    // We're going to do direct updates to the row rather than 
    // through any update mechanism (doesn't exist yet as of this writing).

    let lockedTable = new ComputedTable(table);

    row.put(newData);

    expect(lockedTable.getData()).toEqual([expectedData]);
    expect(table.getData()).toEqual([newData]);
  })

  test("returns only rows available at lock time", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);

    let lockedTable = new ComputedTable(table);

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

    let lockedTable = new ComputedTable(table);

    table.rows[1].delete();

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
  })
})