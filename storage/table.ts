import { CellData } from "./cell";
import Row from "./row";

class Table {
  name:string;
  columns:Array<String> = [];
  rows:Array<Row> = [];
  
  constructor(name:string, columns:Array<String>) {
    this.name = name;
    this.columns = columns
  }

  insert(values:Array<CellData>) {
    if (values.length != this.columns.length) {
      throw new Error("Unexpected number of columns inserted into table " + this.name);
    }

    this.rows.push(
      new Row(values)
    );
  }
}

export default Table;