import Cell, { CellData } from "./cell";
import { Commit, Committed, newCommit } from "./commit";

class Row extends Committed {
  cells:Array<Cell> = [];

  constructor(data:Array<CellData>, commit?:Commit) {
    super("row", commit);

    data.forEach((value) => {
      this.cells.push(new Cell(value, this.createdAt));
    })
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

    return result;
  }
}

export default Row; 