import { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";
import Row from "./row";

class Table extends Committed {
  name:string;
  columns:Array<String> = [];
  rows:Array<Row> = [];
  
  constructor(name:string, columns:Array<String>, commit?:Commit) {
    super("table", commit);

    this.name = name;
    this.columns = columns
  }

  insert(values:Array<CellData>) {
    if (values.length != this.columns.length) {
      throw new Error("Unexpected number of columns inserted into table " + this.name);
    }

    this.rows.push(
      new Row(values)
    );
  }

  project(columns:Array<String>) {
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

export class ComputedTable extends Table {
  lockedAt:Commit; 
  projectedColumnIndexes:Array<number>|null = null;

  constructor(table:Table, projectedColumns:Array<String>|null = null, commit?:Commit) {
    super(table.name, table.columns, table.createdAt);
    this.rows = table.rows;

    if (projectedColumns != null) {
      this.projectedColumnIndexes = projectedColumns.map((columnName) => {
        // TODO: This indexOf can be made faster if we maintained a
        // column name to index map. 
        let possibleIndex = this.columns.indexOf(columnName);

        if (possibleIndex < 0) {
          throw new Error("Column " + columnName + " does not exist in table " + this.name);
        }

        return possibleIndex;
      }); 
    }

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

    if (this.projectedColumnIndexes == null) {
      return this.getRows(commit).map((row) => {
        return row.getData(commit);
      })
    } else {
      return this.getRows(commit).map((row) => {
        return (this.projectedColumnIndexes as Array<number>).map((index) => {
          return row.cells[index].getData(commit);
        });
      });
    }
  }
}

export default Table;