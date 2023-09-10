import Database from "./storage/database";

class Storage {
  databases:Record<string, Database> = {};
  defaultDatabase:string = "default"; 

  constructor() {
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
};

export default Storage;