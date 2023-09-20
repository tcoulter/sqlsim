import compute, { stringifyExpression } from "./compute";
import { ColumnRef } from "./execute";
import { CellData } from "./storage/cell";
import ColumnIndexMap from "./storage/columnindexmap";
import { Commit } from "./storage/commit";
import Database from "./storage/database";
import Row from "./storage/row";
import Table from "./storage/table";

type StorageStackItem = {
  row:Row,
  columnIndexMap:ColumnIndexMap
}

class Storage {
  databases:Record<string, Database> = {};
  defaultDatabase:string = "default"; 

  execStack:Array<StorageStackItem>;

  constructor() {
    this.execStack = [];
    this.createDatabase(this.defaultDatabase);
  }

  hasDatabase(name:string) {
    return typeof this.databases[name] != "undefined";
  }

  createDatabase(name:string) {
    if (this.hasDatabase(name)) {
      throw new Error("Database with name " + name + " already exists!");
    }

    this.databases[name] = new Database(name);
  }

  getDatabase(name:string) {
    if (this.hasDatabase(name) == false) {
      throw new Error("No database with name " + name + " within storage!");
    }

    return this.databases[name];
  }

  pushStack(row:Row, columnIndexMap:ColumnIndexMap) {
    this.execStack.push({
      row,
      columnIndexMap
    })
  }

  popStack() {
    if (this.execStack.length == 0) {
      throw new Error("Tried to pop from stack of size 0");
    }

    this.execStack.pop();
  }

  // Go through each member in the stack seeing if it can find the data requested
  getStackData(column:ColumnRef, commit?:Commit):CellData|undefined {
    if (column.table == null) {
      return undefined;
    }

    for (var index = this.execStack.length - 1; index >= 0; index--) {
      let stackItem = this.execStack[index];

      if (stackItem.columnIndexMap.hasColumn({expr: column, as: null})) {
        return compute(column, stackItem.row, stackItem.columnIndexMap, this, commit);
      } 
    }

    return undefined;
  }
};

export default Storage;