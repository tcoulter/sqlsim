import { AggregateExpression, BinaryExpression, stringifyExpression } from "../compute";
import { Column } from "../execute";

export type AllowedColumnMapping = number|BinaryExpression|AggregateExpression;

type TableAndColumnName = {
  tableName:string|null,
  columnName:string
}

class UniqueMappingManager {
  map:Map<number|string, AllowedColumnMapping>;

  constructor() {
    this.map = new Map();
  }

  add(value:AllowedColumnMapping) {
    let key:string|number;

    if (typeof value == "number") {
      key = value;
    } else {
      key = stringifyExpression(value);
    }

    this.map.set(key, value)
  }

  count() {
    return this.map.size;
  }

  getOne() {
    return this.getAll()[0];
  }

  getAll() {
    return Array.from(this.map.values());
  }

  merge(mappings:UniqueMappingManager) {
    mappings.getAll().forEach((value) => {
      this.add(value);
    })
  }
}

// TODO: This has gotten so complex. Maybe there's a way to simplify it. 
export default class ColumnIndexMap {
  columnNameMap:Record<string, UniqueMappingManager>;
  tableColumnNameMap:Record<string, Record<string, UniqueMappingManager>>; 

  constructor(existingMap?:ColumnIndexMap) {
    this.columnNameMap = {};
    this.tableColumnNameMap = {};

    if (typeof existingMap != "undefined") {
      this.merge(existingMap);
    } 
  }

  setColumn(column:Column, index:AllowedColumnMapping) {
    let columnName = stringifyExpression(column.expr);
    let columnNameAlias:string|null = column.as;

    // Add column names and prefixes, 
    
    
    if (columnNameAlias != null) {
      this.#addToColumnNameMap(columnNameAlias, index);
    } else {
      this.#addToColumnNameMap(columnName, index);
    }

    if (column.expr.type == "column_ref") {
      if (column.expr.table != null) {
        this.#addToTableColumnNameMap(column.expr.table, column.expr.column, index)
      }
    }
  }

  hasColumn(column:Column|string) {
    let {tableName, columnName} = this.#getTableAndColumnNames(column);
    return this.#getMappings(tableName, columnName) != undefined;
  }

  getColumnMapping(column:Column|string):AllowedColumnMapping|undefined {
    let {tableName, columnName} = this.#getTableAndColumnNames(column);

    let mappings = this.#getMappings(tableName, columnName);

    if (typeof mappings == "undefined") {
      return undefined;
    }

    if (mappings.count() > 1) {
      throw new Error("Column '" + (tableName != null ? tableName + "." : "") + columnName + "' is ambiguous.");
    }

    return mappings.getOne();
  }

  mutate(fn:(tableName:string|null, columnName:string, value:AllowedColumnMapping) => AllowedColumnMapping) {
    Object.entries(this.columnNameMap).forEach(([columnName, mappings]) => {
      
      let newMappings = new UniqueMappingManager();
      mappings.getAll().forEach((value) => newMappings.add(fn(null, columnName, value)));
      this.columnNameMap[columnName] = newMappings;
    })

    Object.entries(this.tableColumnNameMap).forEach(([tableName, columnRecord]) => {
      Object.entries(columnRecord).forEach(([columnName, mappings]) => {
        let newMappings = new UniqueMappingManager();
        mappings.getAll().forEach((value) => newMappings.add(fn(tableName, columnName, value)));
        this.tableColumnNameMap[tableName][columnName] = newMappings;
      })
    })
  }

  merge(otherMap:ColumnIndexMap, ignoreIfMappingPresent = false) {
    Object.entries(otherMap.columnNameMap).forEach(([columnName, mappings]) => {
      let existingMappings:UniqueMappingManager;

      if (typeof this.columnNameMap[columnName] != "undefined") {
        if (ignoreIfMappingPresent == true) {
          return;
        }

        existingMappings = this.columnNameMap[columnName];
      } else {
        existingMappings = new UniqueMappingManager();
      }

      existingMappings.merge(mappings);

      this.columnNameMap[columnName] = existingMappings;
    })

    Object.entries(otherMap.tableColumnNameMap).forEach(([tableName, columnRecord]) => {
      Object.entries(columnRecord).forEach(([columnName, mappings]) => {
        let existingMappings:UniqueMappingManager;

        if (typeof this.tableColumnNameMap[tableName] == "undefined") {
          this.tableColumnNameMap[tableName] = {};
        }
  
        if (typeof this.tableColumnNameMap[tableName][columnName] != "undefined") {
          if (ignoreIfMappingPresent == true) {
            return;
          }

          existingMappings = this.tableColumnNameMap[tableName][columnName];
        } else {
          existingMappings = new UniqueMappingManager();
        }
  
        existingMappings.merge(mappings);
  
        this.tableColumnNameMap[tableName][columnName] = existingMappings;
      })
    });
  }

  #getTableAndColumnNames(column:Column|string):TableAndColumnName {
    let tableName:string|null = null;
    let columnName:string;

    if (typeof column == "string") {
      columnName = column;
    } else {
      columnName = column.as || stringifyExpression(column.expr);

      if (column.expr.type == "column_ref") {
        tableName = column.expr.table;
      }
    }

    return {
      tableName,
      columnName
    }
  }

  #getMappings(tableName:string|null, columnName:string):UniqueMappingManager|undefined {
    // If we're given only a column name with no table reference, let's get it from 
    // columnNameMap.
    let mappings:UniqueMappingManager|undefined;
    if (tableName == null) {
      mappings = this.columnNameMap[columnName]; 
    } else {
      let tableRecord = this.tableColumnNameMap[tableName];

      if (typeof tableRecord == "undefined") {
        throw new Error("Unknown table " + tableName);
      }

      // We were given a column reference. Let's try to look it up directly. 
      mappings = this.tableColumnNameMap[tableName][columnName];
    }

    return mappings;
  }

  #addToColumnNameMap(columnName:string, index:AllowedColumnMapping) {
    if (typeof this.columnNameMap[columnName] == "undefined") {
      this.columnNameMap[columnName] = new UniqueMappingManager();
    }

    this.columnNameMap[columnName].add(index);
  }

  #addToTableColumnNameMap(tableName:string, columnName:string, index:AllowedColumnMapping) {
    if (typeof this.tableColumnNameMap[tableName] == "undefined") {
      this.tableColumnNameMap[tableName] = {};
    }

    if (typeof this.tableColumnNameMap[tableName][columnName] == "undefined") {
      this.tableColumnNameMap[tableName][columnName] = new UniqueMappingManager();
    }

    this.tableColumnNameMap[tableName][columnName].add(index);
  }

  static merge(mapOne:ColumnIndexMap, mapTwo:ColumnIndexMap, ignoreIfMappingPresent = false) {
    let fullMap = new ColumnIndexMap(); 

    fullMap.merge(mapOne, ignoreIfMappingPresent);
    fullMap.merge(mapTwo, ignoreIfMappingPresent);

    return fullMap;
  }
}