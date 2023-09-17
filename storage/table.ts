import { firstBy } from "thenby";
import compute, { AggregateExpression, BinaryExpression, SingleExpression, computeAggregateName, computeAggregates, extractAggregateExpressions, stringifyExpression } from "../compute";
import { Column, ColumnRef, OrderBy, createWhereFilter } from "../execute";
import { CellData } from "./cell";
import { Commit, Committed, getLatestCommit, newCommit } from "./commit";
import Row, { JoinedRow } from "./row";

export type ColumnIndexMap = Record<string, number|BinaryExpression|AggregateExpression>;

export default class Table extends Committed {
  name:string;
  columns:Array<Column> = [];
  #rows:Array<Row> = [];
  columnIndexMap:ColumnIndexMap = {};
  sourceDataCellCount:number;
  
  constructor(name:string, columns:Array<Column|string>, commit?:Commit) {
    super("table", commit);

    this.name = name;
    this.columns = columns.map((column) => {
      if (typeof column == "string") {
        return {
          expr: {
            type: "column_ref", 
            column: column,
            table: null
          },
          as: null
        };
      } else {
        return column;
      }
    });

    this.sourceDataCellCount = columns.length;

    // Note that all base tables have only indeces within their columnIndexMap. 
    this.columns.forEach((column, index) => {
      // Note column.as should always be null here because this is table creation
      let columnName = stringifyExpression(column.expr);
      this.columnIndexMap[columnName] = index;
    })
  }

  insert(cellOrRowData:Array<CellData>|Array<Array<CellData>>, columns:Array<string|Column> = this.columns) {
    if (cellOrRowData.length == 0) {
      throw new Error("Insert called with no data!");
    }

    let commit = newCommit();

    // Single row? 
    if (!Array.isArray(cellOrRowData[0])) {
      cellOrRowData = [cellOrRowData as Array<CellData>];
    }

    (cellOrRowData as Array<Array<CellData>>).forEach((rowData:Array<CellData>) => {
      if (rowData.length != columns.length) {
        throw new Error("Not enough data passed to insert() for given columns")
      }
    
      // Create new row filled with nulls
      let newRow = new Row(new Array(this.columns.length).fill(null), commit);

      columns.forEach((column, index) => {
        // Get the value based on its position passed
        let newValue = rowData[index];

        let columnName:string; 
        if (typeof column == "string") {
          columnName = column;
        } else {
          // TODO: Enforce only Column<ColumnRef>'s were passed.
          columnName = (column as Column<ColumnRef>).expr.column;
        }

        // Now use the columnIndexMap to update the columns in the row
        newRow.cell(this.columnIndexMap[columnName] as number).put(newValue, commit);
      });

      this.#rows.push(newRow);
    })
  }

  update(columns:Array<string>, values:Array<CellData|SingleExpression>, where?:BinaryExpression) {
    let table:Table = this;

    if (columns.length != values.length) {
      throw new Error("Number of values given to update() must match number of columns specified.");
    }

    if (typeof where != "undefined") {
      table = new FilteredTable({
        table,
        rowFilters: [createWhereFilter(where)]
      })
    }

    let rows = table.getRows();

    // TODO: There can be some efficiencies gained when UPDATE values don't have expressions
    // in them (e.g., no need to run the inner loop on every row). But we don't care for now. 
    rows.forEach((row) => {
      columns.forEach((column, index) => {
        let value = values[index];

        // We need to discern between cell data and binary expression. 
        // A binary expression is an object. So is null... 
        if (value != null && typeof value == "object") {
          value = compute(value as BinaryExpression, row, this.columnIndexMap)
        }

        // Get the index of the column to update
        let cellIndex = this.columnIndexMap[column] as number;

        // Update the value! 
        row.cell(cellIndex).put(value as CellData);
      });
    })
  }

  project(columns:Array<string>):ProjectedTable {
    // Lock it like it's hot
    return new ProjectedTable({
      table: this, 
      columns: columns.map((columnName) => {
        // TODO: This is a hack! We'll eventually want to refactor everything to use
        // column references instead of strings. 
        return {
          expr: {
            type: "column_ref",
            table: null,
            column: columnName
          },
          as: null
        }
      }),
    });
  }

  hasColumn(columnName:string) {
    return typeof this.columnIndexMap[columnName] != "undefined";
  }

  getRows(commit?:Commit):Array<Row> {
    return this.#rows.filter((row) => {
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

export class LockedTable extends Table {
  baseTable:Table;
  lockedAt:Commit; 

  constructor(table:Table, commit?:Commit) {
    super(table.name, table.columns, table.createdAt);
    this.baseTable = table;
    this.columnIndexMap = table.columnIndexMap;
    this.sourceDataCellCount = table.sourceDataCellCount;
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

    return this.baseTable.getRows(commit);
  }

  getData(commit?:Commit):Array<Array<CellData>> {
    if (typeof commit == "undefined") {
      commit = this.lockedAt;
    }

    if (commit > this.lockedAt) {
      commit = this.lockedAt
    }

    let rows = this.getRows(commit);

    // Please note that this block does the bulk of the work for 
    // projected tables. 
    return rows.map((row) => {
      return this.columns.map((column) => {
        return compute(column.expr, row, this.columnIndexMap, commit);
      });
    });
  }
}

export type RowFilter = (table:FilteredTable, row:Row) => boolean;

export type FilteredTableOptions = {
  table: Table, 
  rowFilters?: Array<RowFilter>,
  commit?:Commit
};

export class FilteredTable extends LockedTable {
  rowFilters:Array<RowFilter>;

  constructor({table, rowFilters = [], commit}:FilteredTableOptions) {
    super(table);

    this.baseTable = table;

    this.rowFilters = rowFilters;
    this.lockedAt = commit || getLatestCommit();
  }

  getRows(commit?:Commit):Array<Row> {
    let rows = super.getRows(commit);

    // Perform row filters. This is where WHERE clause processing happens. 
    if (this.rowFilters.length > 0) {
      this.rowFilters.forEach((rowFilter) => {
        rows = rows.filter((row) => {
          return rowFilter(this, row);
        })
      });
    }

    return rows;
  }
}

export type ComputedColumnsList = Record<string, BinaryExpression|AggregateExpression>;

export type ProjectedTableOptions = {
  table: Table,
  columns: Array<Column>,
  computedColumns?: ComputedColumnsList,
  commit?:Commit
}

export class ProjectedTable extends LockedTable {
  constructor({table, columns}:ProjectedTableOptions) {
    super(table);
    this.columns = [];
    
    this.columnIndexMap = Object.assign(table.columnIndexMap);

    let currentColumnIndex = 0;
    columns.forEach((column) => {
      let stringified = stringifyExpression(column.expr);
      let name = column.as || stringified;

      // If we have a binary expression or aggregate function, AND neither
      // has a column in the original table, then lets make the column index
      // map point to the expression as a computed column. 
      // Note that the Object.assign() above takes care of the case where 
      // the expression is already in the index map.  
      if (column.expr.type == "binary_expr" || column.expr.type == "aggr_func") {
        // Only attempt to recalculate if an index for this expression isn't set
        // e.g., it will be set on aggregates
        if (typeof table.columnIndexMap[stringified] == "undefined") {
          this.columnIndexMap[stringified] = column.expr;
        }
      }
      
      // If our column has an AS keyword, let's remap the new name onto the index
      // for the old one. Note that stringified is the old name, and name is the
      // new one. 
      if (column.as != null)   {
        this.columnIndexMap[name] = table.columnIndexMap[stringified]
      }

      // Finally, let's build our list of column names
      if (column.expr.type == "column_ref" && column.expr.column == "*") {
        this.columns.push.apply(this.columns, table.columns);
        currentColumnIndex += table.columns.length;
      } else {
        this.columns.push(column);
        currentColumnIndex += 1;
      }
    });
  }
}


export type JoinType = "left" | "right" | "inner" | "full";

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
  left:Table;
  right:Table;
  on:BinaryExpression;

  constructor({type, left, right, on, commit}:JoinedTableOptions) {
    let columns = left.columns.concat(right.columns);
    super("Joined Table", columns, left.createdAt);
    this.type = type;

    this.left = new LockedTable(left);
    this.right = new LockedTable(right);

    this.on = on;

    this.lockedAt = commit || getLatestCommit();

    this.sourceDataCellCount = left.sourceDataCellCount + right.sourceDataCellCount;

    // Write a columnIndexMap that accounts for both rows
    // Note that the indexes refer to the index value as if
    // the cell data were one big array. 
    let newColumnIndexMap:ColumnIndexMap = Object.assign({}, left.columnIndexMap);

    // Now add in the right columns, adjusting index to account for left values
    Object.entries(right.columnIndexMap).forEach(([rightColumn, rightColumnIndex]) => {
      if (typeof rightColumnIndex == "number") {
        newColumnIndexMap[rightColumn] = left.sourceDataCellCount + rightColumnIndex;
      } else {
        // If binary expression, copy the expression
        newColumnIndexMap[rightColumn] = rightColumnIndex;
      }
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

    let leftRows = this.left.getRows(commit); 
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
          newRows.push(new JoinedRow(
            new Row(new Array(this.left.sourceDataCellCount).fill(null)),
            rightRow
          ))
        }
      });
    }

    return newRows;
  }
}

export class AggregateTable extends Table {
  #aggregates:Array<AggregateExpression>;
  baseTable:Table;
  groupColumns:Array<ColumnRef>;

  constructor(aggregates:Array<AggregateExpression>, table:Table, groupColumns:Array<ColumnRef> = []) {
    let aggregateNames = aggregates.map((aggregate) => {
      return stringifyExpression(aggregate);
    });

    // TODO: Is this a hack? 
    let columnNames = table.columns.concat(aggregates.map((aggregate) => {
      return {
        expr: aggregate,
        as: null
      }
    }));

    super(table.name, columnNames, table.createdAt);

    this.#aggregates = aggregates;
    this.baseTable = table;
    this.groupColumns = groupColumns;

    // Write a column index map that includes the base table plus our aggregates
    // We'll update our source data cell count at the same time. 
    this.columnIndexMap = Object.assign({}, table.columnIndexMap);
    this.sourceDataCellCount = table.sourceDataCellCount;

    aggregateNames.forEach((aggregateName) => {
      let newIndex = this.sourceDataCellCount;
      this.columnIndexMap[aggregateName] = newIndex;
      this.sourceDataCellCount += 1;
    })
  }

  getRows(commit?:Commit):Array<Row> {
    return computeAggregates({
      aggregates: this.#aggregates,
      rows: this.baseTable.getRows(commit),
      columnIndexMap: this.baseTable.columnIndexMap,
      groupColumns: this.groupColumns,
      commit
    });
  }
}

// This class will return the first row found for every distinct
// combination of the distinct columns. 
export class DistinctTable extends Table {
  baseTable:Table;
  distinctColumns:Array<ColumnRef>;
  
  // TODO: Expand distinctColumns to support naming columns via expressions (e.g., if AS is not used)
  constructor(table:Table, distinctColumns:Array<ColumnRef> = []) {
    super(table.name, table.columns, table.createdAt);

    this.columnIndexMap = table.columnIndexMap
    this.baseTable = table; 
    this.distinctColumns = distinctColumns;
  }

  getRows(commit?:Commit):Array<Row> {
    let distinctRowsByHash:Record<string, Row> = {};

    this.baseTable.getRows(commit).forEach((row) => {
      let distinctCombination = this.distinctColumns.map((column) => {
        return row.cell(this.baseTable.columnIndexMap[column.column] as number).getData(commit)
      })

      let hash = JSON.stringify(distinctCombination);

      if (typeof distinctRowsByHash[hash] == "undefined") {
        distinctRowsByHash[hash] = row;
      }
    });

    return Object.values(distinctRowsByHash);
  }
}

export class OrderedTable extends Table {
  baseTable:Table;
  ordering:Array<OrderBy>;

  // TODO: Expand distinctColumns to support naming columns via expressions (e.g., if AS is not used)
  constructor(table:Table, ordering:Array<OrderBy>) {
    super(table.name, table.columns, table.createdAt);
    this.baseTable = table;
    this.ordering = ordering;

    if (ordering.length == 0) {
      throw new Error("OrderedTable must have at least one sort parameter");
    } 
  }

  getRows(commit?:Commit):Array<Row> {
    let rows = this.baseTable.getRows(commit);

    let createSortFunction = (item:OrderBy) => {
      return (row:Row) => {
        return compute(item.expr, row, this.baseTable.columnIndexMap, commit);
      }
    }

    let sorted = rows.sort(
      this.ordering.reduce((fn, item, index) => {
        // Ignore the first item in the list because we used firstBy() on it already.
        if (index > 0) {
          return fn.thenBy(createSortFunction(item), item.type == "DESC" ? -1 : 1)
        }
        return fn;
      }, firstBy(createSortFunction(this.ordering[0]), this.ordering[0].type == "DESC" ? -1 : 1))
    )

    return sorted;
  }
}