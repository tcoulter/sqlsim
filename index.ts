import Storage from "./storage";
import execute from "./execute";

let SQLSim = {
  run: (sql:string, storage?:Storage) {
    storage = storage || new Storage();

    return execute(sql, storage);
  }
}

export default SQLSim;
