import Cell, { CellData } from "./cell";
import { Commit, newCommit } from "./commit";

class Row {
  createdAt:Commit;
  deletedAt:Commit|null = null;

  cells:Array<Cell> = [];

  constructor(data:Array<CellData>) {
    this.createdAt = newCommit();

    data.forEach((value) => {
      this.cells.push(new Cell(value, this.createdAt));
    })
  }

  isDeleted() {
    return this.deletedAt != null;
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
}

export default Row; 