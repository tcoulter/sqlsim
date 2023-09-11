import { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";
import Row from "./row";

class Table extends Committed {
  name:string;
  columns:Array<string> = [];
  rows:Array<Row> = [];
  
  constructor(name:string, columns:Array<string>, commit?:Commit) {
    super("table", commit);

    this.name = name;
    this.columns = columns;
  }

  insert(values:Array<CellData>) {
    if (values.length != this.columns.length) {
      throw new Error("Unexpected number of columns inserted into table " + this.name);
    }

    this.rows.push(
      new Row(values)
    );
  }

  project(columns:Array<string>) {
    // Lock it like it's hot
    return new ComputedTable(this, columns);
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
  columnIndexMap:Record<string, number> = {};

  constructor(table:Table, projectedColumns?:Array<string>, commit?:Commit) {
    let columns = projectedColumns || table.columns;
    super(table.name, columns, table.createdAt);

    // Our computed table will use the given table as its data source
    this.rows = table.rows;

    // Write the a columnIndexMap to map column names to source columns
    // This is O(n^2) but there's a low amount of columns almost always
    columns.forEach((columnName) => {
      let sourceColumnIndex = table.columns.indexOf(columnName);

      if (sourceColumnIndex < 0) {
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
        let sourceIndex = this.columnIndexMap[columnName];
        return row.cells[sourceIndex].getData(commit);
      });
    });
  }
}

export default Table;