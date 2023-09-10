import Table from "./table";

export default class Database {
  tables:Record<string, Table> = {};
  name:string;

  constructor(name:string) {
    this.name = name;
  }

  hasTable(tableName:string) {
    return typeof this.tables[tableName] != "undefined";
  }

  createTable(tableName:string, columnNames:Array<string>) {
    // TODO: Support IF NOT EXISTS, etc.
    if (this.hasTable(tableName)) {
      throw new Error("Table " + tableName + " already exists in database!");
    }

    this.tables[tableName] = new Table(tableName, columnNames);

    return this.tables[tableName];
  }

  getTable(tableName:string) {
    if (!this.hasTable(tableName)) {
      throw new Error("Cannot get table " + tableName + " because it doesn't exist in database!");
    }

    return this.tables[tableName];
  }
}