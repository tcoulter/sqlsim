import { CellData } from "../storage/cell";
import Row from "../storage/row";
import Table, { LockedTable } from "../storage/table";

describe("LockedTable", () => {
  test("cells return data at lock time even after updates", () => {
    let expectedData:Array<CellData> = ["Tim", 30];
    let newData:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    let row = new Row(expectedData);

    table.rows.push(row);

    // We're going to do direct updates to the row rather than 
    // through any update mechanism (doesn't exist yet as of this writing).

    let lockedTable = new LockedTable(table);

    row.put(newData);

    expect(lockedTable.getData()).toEqual([expectedData]);
    expect(table.getData()).toEqual([newData]);
  })

  test("locked tables return only rows available at lock time", () => {
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

  test("locked tables return rows available at lock time that were later deleted", () => {
    let firstRow:Array<CellData> = ["Tim", 30];
    let secondRow:Array<CellData> = ["Timmma", 31];

    let table = new Table("People", ["name", "age"]);
    table.insert(firstRow);
    table.insert(secondRow);

    let lockedTable = new LockedTable(table);

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