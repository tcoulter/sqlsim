import { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";
import Row from "./row";

export type ColumnIndexMap = Record<string, number>;

export default class Table extends Committed {
  name:string;
  columns:Array<string> = [];
  rows:Array<Row> = [];
  columnIndexMap:ColumnIndexMap = {};
  
  constructor(name:string, columns:Array<string>, commit?:Commit) {
    super("table", commit);

    this.name = name;
    this.columns = columns;

    this.columns.forEach((columnName, index) => {
      this.columnIndexMap[columnName] = index;
    })
  }

  insert(cellOrRowData:Array<CellData>|Array<Array<CellData>>) {
    // Single row? 
    if (!Array.isArray(cellOrRowData[0])) {
      cellOrRowData = [cellOrRowData as Array<CellData>];
    }

    (cellOrRowData as Array<Array<CellData>>).forEach((values) => {
      if (values.length != this.columns.length) {
        throw new Error("Unexpected number of columns inserted into table " + this.name);
      }
  
      this.rows.push(
        new Row(values)
      );
    })
  }

  project(columns:Array<string>) {
    // Lock it like it's hot
    return new ComputedTable(this, columns);
  }

  hasColumn(columnName:string) {
    return typeof this.columnIndexMap[columnName] != "undefined";
  }

  getRows(commit?:Commit):Array<Row> {
    return this.rows.filter((row) => {
      // If no commit, return the row. 
      if (typeof commit == "undefined") {
        return !row.isDeleted();
      }

      // If a commit was passed, only return this row
      // if the row wasn't deleted at this commit
      // and the row was created before the expected commit. 
      return !row.isDeleted(commit) && row.createdAt <= commit;
    })
  }

  getData(commit?:Commit):Array<Array<CellData>> {
    return this.getRows(commit).map((row) => {
      return row.getData(commit);
    })
  } 
}

export type RowFilter = Function;

export type ComputedTableOptions = {
  table: Table, 
  projectedColumns?:Array<string>,
  rowFilters: Array<RowFilter>,
  commit?:Commit
};

export class ComputedTable extends Table {
  lockedAt:Commit; 

  constructor(table:Table, projectedColumns?:Array<string>, commit?:Commit) {
    let columns = projectedColumns || table.columns;
    super(table.name, columns, table.createdAt);

    // Our computed table will use the given table as its data source
    this.rows = table.rows;

    // Rewrite the a columnIndexMap to to point to source column indexes
    // This is O(n^2) but there's a low amount of columns almost always
    columns.forEach((columnName) => {
      let sourceColumnIndex = table.columnIndexMap[columnName];

      if (typeof sourceColumnIndex == "undefined") {
        throw new Error("Column " + columnName + " does not exist in table " + this.name);
      }

      this.columnIndexMap[columnName] = sourceColumnIndex;
    })

    this.lockedAt = commit || getLatestCommit();
  }

  insert(values:Array<CellData>) {
    throw new Error("Locked tables are immutable"); 
  }

  getData(commit?:Commit):Array<Array<CellData>> {
    if (typeof commit == "undefined") {
      commit = this.lockedAt;
    }

    if (commit > this.lockedAt) {
      commit = this.lockedAt
    }

    return this.getRows(commit).map((row) => {
      return this.columns.map((columnName) => {
        return row.cells[this.columnIndexMap[columnName]].getData(commit);
      });
    });
  }
}