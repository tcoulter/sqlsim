import Cell, { CellData } from "./cell";
import { Commit, newCommit } from "./commit";

class Row {
  createdAt:Commit;
  deletedAt:Commit|null = null;

  cells:Array<Cell> = [];

  constructor(data:Array<CellData>, commit?:Commit) {
    this.createdAt = commit || newCommit();

    data.forEach((value) => {
      this.cells.push(new Cell(value, this.createdAt));
    })
  }

  isDeleted(commit?:Commit) {
    // If it hasn't been deleted at all, always return false. 
    if (this.deletedAt == null) {
      return false;
    }

    // deletedAt must be set. 
    // If no commit was asked for, then we're looking for the
    // latest state, which is deleted. 
    if (typeof commit == "undefined") {
      return true;
    }

    // Else we have a commit; only return true if commit is 
    // greater than the deleted time. 
    return commit > this.deletedAt;
  }

  delete(commit?:Commit) {
    if (this.isDeleted()) {
      throw new Error("Cannot delete row. It's already deleted!");
    }

    this.deletedAt = typeof commit != "undefined" ? commit : newCommit();
  }

  // TODO: Expand this to do actual projection rather than
  // just column filtering. 
  project(columnIndexes:Array<number>) {
    let newCells:Array<Cell> = [];

    columnIndexes.sort().forEach((index) => {
      newCells.push(this.cells[index]);
    })

    return newCells;
  }

  put(data:Array<CellData>, commit:Commit = newCommit()) {
    if (data.length != this.cells.length) {
      throw new Error("Unexpected number of values passed to row's put()");
    }

    data.forEach((value, index) => {
      this.cells[index].put(value, commit);
    });
  }

  getData(commit?:Commit):Array<CellData> {
    let result = this.cells.map((cell) => {
      return cell.getData(commit);
    })

    console.log("ROW", result);

    return result;
  }
}

export default Row; 