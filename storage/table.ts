import compute, { BinaryExpression, ComputationResult } from "../compute";
import { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";
import Row, { JoinedRow } from "./row";

export type ColumnIndexMap = Record<string, number>;

export default class Table extends Committed {
  name:string;
  columns:Array<string> = [];
  rows:Array<Row> = [];
  columnIndexMap:ColumnIndexMap = {};
  sourceDataCellCount:number;
  
  constructor(name:string, columns:Array<string>, commit?:Commit) {
    super("table", commit);

    this.name = name;
    this.columns = columns;
    this.sourceDataCellCount = columns.length;

    this.columns.forEach((columnName, index) => {
      this.columnIndexMap[columnName] = index;
    })
  }

  insert(cellOrRowData:Array<CellData>|Array<Array<CellData>>) {
    // Single row? 
    if (!Array.isArray(cellOrRowData[0])) {
      cellOrRowData = [cellOrRowData as Array<CellData>];
    }

    (cellOrRowData as Array<Array<CellData>>).forEach((values) => {
      if (values.length != this.columns.length) {
        throw new Error("Unexpected number of columns inserted into table " + this.name);
      }
  
      this.rows.push(
        new Row(values)
      );
    })
  }

  project(columns:Array<string>) {
    // Lock it like it's hot
    return new ComputedTable({
      table: this, 
      projectedColumns: columns
    });
  }

  hasColumn(columnName:string) {
    return typeof this.columnIndexMap[columnName] != "undefined";
  }

  getRows(commit?:Commit):Array<Row> {
    return this.rows.filter((row) => {
      // If no commit, return the row. 
      if (typeof commit == "undefined") {
        return !row.isDeleted();
      }

      // If a commit was passed, only return this row
      // if the row wasn't deleted at this commit
      // and the row was created before the expected commit. 
      return !row.isDeleted(commit) && row.createdAt <= commit;
    })
  }

  getData(commit?:Commit):Array<Array<CellData>> {
    return this.getRows(commit).map((row) => {
      return row.getData(commit);
    })
  } 
}

export type RowFilter = (table:ComputedTable, row:Row) => boolean;

export type ComputedTableOptions = {
  table: Table, 
  projectedColumns?:Array<string>,
  rowFilters?: Array<RowFilter>,
  commit?:Commit
};

export class ComputedTable extends Table {
  lockedAt:Commit; 
  rowFilters:Array<RowFilter>;

  constructor({table, projectedColumns, rowFilters = [], commit}:ComputedTableOptions) {
    let columns = projectedColumns || table.columns;
    super(table.name, columns, table.createdAt);

    // Our computed table will use the given table as its data source
    this.rows = table.rows;

    // Rewrite the a columnIndexMap to to point to source column indexes
    // This is O(n^2) but there's a low amount of columns almost always
    columns.forEach((columnName) => {
      let sourceColumnIndex = table.columnIndexMap[columnName];

      if (typeof sourceColumnIndex == "undefined") {
        throw new Error("Column " + columnName + " does not exist in table " + this.name);
      }

      this.columnIndexMap[columnName] = sourceColumnIndex;
    })

    this.rowFilters = rowFilters;
    this.lockedAt = commit || getLatestCommit();
  }

  insert(values:Array<CellData>) {
    throw new Error("Locked tables are immutable"); 
  }

  getRows(commit?:Commit):Array<Row> {
    if (typeof commit == "undefined") {
      commit = this.lockedAt;
    }

    if (commit > this.lockedAt) {
      commit = this.lockedAt
    }

    return super.getRows(commit);
  }

  getData(commit?:Commit):Array<Array<CellData>> {
    if (typeof commit == "undefined") {
      commit = this.lockedAt;
    }

    if (commit > this.lockedAt) {
      commit = this.lockedAt
    }

    let rows = this.getRows(commit);

    // Perform row filters. This is where WHERE clause processing happens. 
    if (this.rowFilters.length > 0) {
      this.rowFilters.forEach((rowFilter) => {
        rows = rows.filter((row) => {
          return rowFilter(this, row);
        })
      });
    }

    return rows.map((row) => {
      return this.columns.map((columnName) => {
        return row.cell(this.columnIndexMap[columnName]).getData(commit);
      });
    });
  }
}

export type JoinType = "left" | "right" | "inner" | "full";

// For a JoinedTable, we extend a ComputedTable which 
// acts as the left part of the join. 

export type JoinedTableOptions = {
  type: JoinType,
  left: Table,
  right: Table,
  on: BinaryExpression,
  commit?: Commit
}

export class JoinedTable extends Table {
  lockedAt:Commit; 

  type:JoinType;
  right:Table;
  on:BinaryExpression;

  constructor({type, left, right, on, commit}:JoinedTableOptions) {
    let columns = left.columns.concat(right.columns);
    super("Joined Table", columns, left.createdAt);
    this.rows = left.rows;
    this.type = type;

    this.right = new ComputedTable({
      table: right
    });

    this.on = on;

    this.lockedAt = commit || getLatestCommit();

    this.sourceDataCellCount = left.sourceDataCellCount + right.sourceDataCellCount;

    // Write a columnIndexMap that accounts for both rows
    // Note that the indexes refer to the index value as if
    // the cell data were one big array. 
    let newColumnIndexMap:ColumnIndexMap = {};

    // Keep the left columns
    Object.keys(left.columnIndexMap).forEach((leftColumnName) => {
      newColumnIndexMap[leftColumnName] = left.columnIndexMap[leftColumnName];
    })

    // Now add in the right columns, adjusting index to account for left values
    Object.entries(right.columnIndexMap).forEach(([rightColumn, rightColumnIndex]) => {
      newColumnIndexMap[rightColumn] = left.sourceDataCellCount + rightColumnIndex;
    })
      
    this.columnIndexMap = newColumnIndexMap;
  }

  getRows(commit?:Commit):Array<Row> {
    if (typeof commit == "undefined") {
      commit = this.lockedAt;
    }

    if (commit > this.lockedAt) {
      commit = this.lockedAt
    }

    let leftRows = super.getRows(commit); 
    let rightRows = this.right.getRows(commit);

    let newRows:Array<Row> = [];
    let rightMatches:Map<Row, boolean> = new Map();

    leftRows.forEach((leftRow) => {
      let foundMatchinRightRow = false;

      rightRows.forEach((rightRow) => {
        let joinedRow = new JoinedRow(leftRow, rightRow);
        let rowMatches = !!compute(this.on, joinedRow, this.columnIndexMap, commit);

        if (rowMatches == true) {
          foundMatchinRightRow = true;
          newRows.push(joinedRow);
        } 

        // If we don't yet have a record for the current right row
        // OR, if we do (assumed) AND we haven't found a match yet,
        // record what we have. 
        if (!rightMatches.has(rightRow) || rightMatches.get(rightRow) == false) {
          rightMatches.set(rightRow, rowMatches);
        }
      })

      // No right match found? Then fill the right with null on a left join
      if (foundMatchinRightRow == false && (this.type == "left" || this.type == "full")) {
        // Join a row with the left 
        newRows.push(new JoinedRow(
          leftRow,
          new Row(new Array(this.right.sourceDataCellCount).fill(null))
        ));
      }
    });

    // Now for ever right row where a match that wasn't found, let's include rows for it
    // if we have a right or full join. 
    if (this.type == "right" || this.type == "full") {
      rightMatches.forEach((wasFound, rightRow) => {
        if (wasFound == false) {
          // The left (this table) sourceDataCellCount was updated on intialization
          // to include counts for the right data (to appear as though it is one big array).
          // We have to recalculate it by subtracting. (This is smelly, but whatevs).
          newRows.push(new JoinedRow(
            new Row(new Array(this.sourceDataCellCount - this.right.sourceDataCellCount).fill(null)),
            rightRow
          ))
        }
      });
    }

    return newRows;
  }
}