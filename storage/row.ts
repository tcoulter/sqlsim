import Cell, { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";

export default class Row extends Committed {
  cells:Array<Cell> = [];

  constructor(data:Array<CellData>, commit?:Commit) {
    super("row", commit);

    data.forEach((value) => {
      this.cells.push(new Cell(value, this.createdAt));
    })
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

// export class LockedRow extends Row {
//   lockedAt:Commit;
//   lockedIndexes:Array<number>;

//   constructor(row:Row, lockedIndexes:Array<number>, commit?:Commit) {
//     super([], row.createdAt);
//     this.cells = row.cells;

//     this.lockedAt = commit || getLatestCommit();
//     this.lockedIndexes = lockedIndexes;
//   }

//   put(data:Array<CellData>, commit?:Commit) {
//     throw new Error("Locked rows are immutable.");
//   }

//   getData(commit?:Commit):Array<CellData> {
//     if (typeof commit == "undefined") {
//       commit = this.lockedAt;
//     }

//     if (commit > this.lockedAt) {
//       commit = this.lockedAt
//     }

//     return this.lockedIndexes.map((index) => {
//       return this.cells[index].getData(commit);
//     });
//   }
// }