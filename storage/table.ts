import { CellData } from "./cell";
import { Commit, getLatestCommit } from "./commit";
import Row from "./row";

class Table {
  name:string;
  columns:Array<String> = [];
  rows:Array<Row> = [];
  
  constructor(name:string, columns:Array<String>) {
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

  getData(commit?:Commit):Array<Array<CellData>> {
    return this.rows.filter((row) => {
      // If no commit, return the row. 
      if (typeof commit == "undefined") {
        return !row.isDeleted();
      }

      // If a commit was passed, only return this row
      // if the row wasn't deleted at this commit
      // and the row was created before the expected commit. 
      return !row.isDeleted(commit) && row.createdAt <= commit;
    }).map((row) => {
      return row.getData(commit);
    })
  } 
}

export class LockedTable extends Table {
  lockedAt:Commit; 

  constructor(table:Table, commit?:Commit) {
    super(table.name, table.columns);

    this.lockedAt = commit || getLatestCommit();
    this.rows = table.rows;
  }

  getData(commit?:Commit):Array<Array<CellData>> {
    if (typeof commit == "undefined") {
      commit = this.lockedAt;
    }

    if (commit > this.lockedAt) {
      commit = this.lockedAt
    }

    return super.getData(commit);
  }
}

export default Table;