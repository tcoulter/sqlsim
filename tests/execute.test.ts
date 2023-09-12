import execute from "../execute";
import Storage from "../storage"
import Row from "../storage/row";

describe("execute()", () => {
  test("simple CREATE TABLE", () => {
    let {storage} = execute(`
      CREATE Table t (
        age INT
      );
    `);

    expect(Object.keys(storage.databases).length).toBe(1);

    let database = storage.databases['default'];

    expect(database.hasTable('t')).toBe(true)
    expect(database.tables['t'].columns.length).toBe(1);
    expect(database.tables['t'].columns[0]).toBe('age');
  }); 

  test('simple INSERT INTO', () => {
    let {storage} = execute(`
      CREATE Table t (
        age INT
      );

      INSERT INTO t VALUES (20)
    `);

    let database = storage.databases['default'];
    let table = database.getTable('t');

    let rows:Array<Row> = table.getRows();

    expect(rows.length).toBe(1);
    expect(rows[0].numCells()).toBe(1);
    expect(rows[0].cell(0).getData()).toBe(20);
  })

  test('INSERT with multiple values', () => {
    let {storage} = execute(`
      CREATE Table t (
        age INT
      );

      INSERT INTO t VALUES (21), (22)
    `);

    let database = storage.databases['default'];
    let table = database.getTable('t');

    let rows:Array<Row> = table.getRows();

    expect(rows.length).toBe(2);
    expect(rows[0].cell(0).getData()).toBe(21);
    expect(rows[1].cell(0).getData()).toBe(22);
  })

  test('CREATE and INSERT multiple columns, multiple values', () => {
    let {storage} = execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);
    `);

    let database = storage.databases['default'];

    expect(database.hasTable('l')).toBe(true);

    let table = database.getTable('l');
    let rows:Array<Row> = table.getRows();

    expect(table.columns.length).toBe(2);
    expect(table.columns[0]).toBe('name');
    expect(table.columns[1]).toBe('age');

    expect(rows.length).toBe(2);

    // First row
    expect(rows[0].cell(0).getData()).toBe('Tim');
    expect(rows[0].cell(1).getData()).toBe(30);

    // Second row
    expect(rows[1].cell(0).getData()).toBe('Liz');
    expect(rows[1].cell(1).getData()).toBe(21);
  });

  test('INSERT only some columns (with SELECT to make things easy)', () => {
    let {results, storage} = execute(`
      CREATE Table People (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO People (name) VALUES ('Tim'), ('Liz');

      SELECT * FROM People; 
    `);

    let database = storage.databases['default'];
    let table = database.getTable('People');

    expect(results.length).toBe(3);
    expect(results[2]).toEqual([
      ['Tim', null],
      ['Liz', null]
    ])
  })

  test('CREATE, INSERT and UPDATE without WHERE clause (with SELECT, teaser of tests below)', () => {
    let {results, storage} = execute(`
      CREATE TABLE People (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO People VALUES ('Tim', 30), ('Liz', 21), ('Justin', 17), ('Gary', 49);

      UPDATE People SET name = 'Old Person';

      SELECT * FROM People;
    `);
    
    expect(results.length).toBe(4);
    expect(results[3]).toEqual([
      ['Old Person', 30],
      ['Old Person', 21],
      ['Old Person', 17],
      ['Old Person', 49]
    ]);
  })

  test('CREATE, INSERT and UPDATE multiple columns with expressions (with SELECT, teaser of tests below)', () => {
    let {results, storage} = execute(`
      CREATE TABLE People (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO People VALUES ('Tim', 30), ('Liz', 21), ('Justin', 17), ('Gary', 49);

      UPDATE People SET name = 'Old Person', age = 100 + age WHERE age >= 30;

      SELECT * FROM People;
    `);
    
    expect(results.length).toBe(4);
    expect(results[3]).toEqual([
      ['Old Person', 130],
      ['Liz', 21],
      ['Justin', 17],
      ['Old Person', 149]
    ]);
  })

  test('simple SELECT', () => {
    let {results, storage} = execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);

      SELECT * FROM l;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      ['Tim', 30],
      ['Liz', 21]
    ])
  })

  test('SELECT with column filtering via projection', () => {
    let {results, storage} = execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);

      SELECT name FROM l;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      ['Tim'],
      ['Liz']
    ])
  })

  test('SELECT with column reordering via projection', () => {
    let {results, storage} = execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);

      SELECT age, name FROM l;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      [30, 'Tim'],
      [21, 'Liz']
    ])
  })

  test('SELECT with column expansion via projection', () => {
    let {results, storage} = execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);

      SELECT age, age, age FROM l;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      [30, 30, 30],
      [21, 21, 21]
    ])
  })

  test('SELECT with column expression within projection', () => {
    let {results, storage} = execute(`
      CREATE TABLE People (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO People VALUES ('Tim', 30), ('Liz', 21);

      SELECT age > 25 FROM People;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      [true],
      [false]
    ])
  })

  test('SELECT with multiple column expressions within projection', () => {
    let {results, storage} = execute(`
      CREATE TABLE People (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO People VALUES ('Tim', 30), ('Liz', 21);

      SELECT age, age > 25, age + 4  FROM People;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      [30, true, 34],
      [21, false, 25]
    ])
  })

  test('SELECT with simple WHERE clause', () => {
    let {results, storage} = execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);

      SELECT * FROM l WHERE age > 25;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      ["Tim", 30]
    ])
  });

  test('SELECT with more advanced WHERE clause', () => {
    let {results, storage} = execute(`
      CREATE TABLE People (
        name VARCHAR(20),
        age INT,
        dept VARCHAR(20)
      );

      INSERT INTO People VALUES 
        ('Tim', 30, 'Sales'), 
        ('Liz', 21, 'Sales'),
        ('Preeti', 27, 'Accounting'),
        ('Gary', 18, 'Accounting');

      SELECT * FROM People WHERE dept = 'Accounting' AND age > 25;
    `);

    expect(results.length).toBe(3);
    expect(typeof results[0]).toBe('number');
    expect(typeof results[1]).toBe('number');
    expect(Array.isArray(results[2])).toBe(true);

    expect(results[2]).toEqual([
      ["Preeti", 27, "Accounting"]
    ])
  });

  test('SELECT with joins', () => {
    let {results, storage} = execute(`
      CREATE TABLE People (
        name VARCHAR(20),
        age INTEGER,
        from_id INTEGER
      );
    
      CREATE TABLE Countries (
        country_id INTEGER,
        country_name VARCHAR(20)
      );
    
      INSERT INTO People VALUES ('Tim', 30, 1), ('Liz', 21, 2), ('Russ', 47, NULL);
      INSERT INTO Countries VALUES (1, 'USA'), (2, 'Canada'), (3, 'Mexico');
    
      SELECT * FROM People JOIN Countries ON from_id = country_id;
      SELECT * FROM People LEFT JOIN Countries ON from_id = country_id;
      SELECT * FROM People RIGHT JOIN Countries ON from_id = country_id;
      SELECT * FROM People FULL JOIN Countries ON from_id = country_id;
    `);

    expect(results.length).toBe(8);
    expect(Array.isArray(results[4])).toBe(true);
    expect(Array.isArray(results[5])).toBe(true);
    expect(Array.isArray(results[6])).toBe(true);
    expect(Array.isArray(results[7])).toBe(true);

    // Inner
    expect(results[4]).toEqual([
      ['Tim', 30, 1, 1, 'USA'],
      ['Liz', 21, 2, 2, 'Canada']
    ]);

    // Left
    expect(results[5]).toEqual([
      ['Tim', 30, 1, 1, 'USA'],
      ['Liz', 21, 2, 2, 'Canada'],
      ['Russ', 47, null, null, null]
    ]);

    // Right
    expect(results[6]).toEqual([
      ['Tim', 30, 1, 1, 'USA'],
      ['Liz', 21, 2, 2, 'Canada'],
      [null, null, null, 3, "Mexico"]
    ]);

    // Full
    expect(results[7]).toEqual([
      ['Tim', 30, 1, 1, 'USA'],
      ['Liz', 21, 2, 2, 'Canada'],
      ['Russ', 47, null, null, null],
      [null, null, null, 3, "Mexico"]
    ]);
  })



  // TODO: left, right and full joins (need to study parser)
})