import { ColumnRef } from "../execute";
import { Commit, Committed, getLatestCommit } from "./commit";
import Table from "./table";

export default class Database extends Committed {
  tables:Record<string, Table> = {};
  name:string;

  constructor(name:string, commit?:Commit) {
    super("database", commit);
    this.name = name;
  }

  hasTable(tableName:string, commit:Commit = getLatestCommit()) {
    return typeof this.tables[tableName] != "undefined" 
      && this.tables[tableName].createdAt <= commit;
  }

  createTable(tableName:string, columnNames:Array<string|ColumnRef>) {
    // TODO: Support IF NOT EXISTS, etc.
    if (this.hasTable(tableName)) {
      throw new Error("Table " + tableName + " already exists in database!");
    }

    this.tables[tableName] = new Table(tableName, columnNames);

    return this.tables[tableName];
  }

  // TODO: If a commit is passed, should we return a locked table? 
  getTable(tableName:string, commit:Commit = getLatestCommit()) {
    if (!this.hasTable(tableName, commit)) {
      throw new Error("Cannot get table " + tableName + " because it doesn't exist in database!");
    }

    return this.tables[tableName];
  }
}