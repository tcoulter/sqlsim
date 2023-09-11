import Cell, { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";
import { JoinType } from "./table";

export default class Row extends Committed {
  #cells:Array<Cell> = [];

  constructor(data:Array<CellData>, commit?:Commit) {
    super("row", commit);

    data.forEach((value) => {
      this.#cells.push(new Cell(value, this.createdAt));
    })
  }

  cell(index:number):Cell {
    return this.#cells[index];
  }

  numCells():number {
    return this.#cells.length;
  }

  put(data:Array<CellData>, commit:Commit = newCommit()) {
    if (data.length != this.#cells.length) {
      throw new Error("Unexpected number of values passed to row's put()");
    }

    data.forEach((value, index) => {
      this.#cells[index].put(value, commit);
    });
  }

  getData(commit?:Commit):Array<CellData> {
    let result:Array<CellData> = [];

    //console.log("numCells", this.numCells());

    for (var index = 0; index < this.numCells(); index++) {
      //console.log("--index", index, this.cell(index))
      result.push(this.cell(index).getData(commit));
    }

    return result;
  }
}

export class JoinedRow extends Row {
  left:Row;
  right:Row;

  constructor(left:Row, right:Row) {
    super([]);
    this.left = left;
    this.right = right;
  }

  cell(index:number):Cell {
    if (index < this.left.numCells()) {
      return this.left.cell(index);
    } else {
      return this.right.cell(index - this.left.numCells())
    }
  }

  numCells():number {
    return this.left.numCells() + this.right.numCells();
  }
}