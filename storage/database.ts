import { Column } from "../execute";
import Storage from "../storage";
import { Commit, Committed, getLatestCommit } from "./commit";
import Table, { LockedTable, TableOptions } from "./table";

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

  createTable(options:TableOptions) {
    let {name, columns, storage} = options;
    
    // TODO: Support IF NOT EXISTS, etc.
    if (this.hasTable(name)) {
      throw new Error("Table '" + name + "' already exists in database!");
    }

    this.tables[name] = new Table(options);

    return this.tables[name];
  }

  // TODO: If a commit is passed, should we return a locked table? 
  getTable(tableName:string) {
    if (!this.hasTable(tableName)) {
      throw new Error("Cannot get table " + tableName + " because it doesn't exist in database!");
    }

    return this.tables[tableName];
  }
}