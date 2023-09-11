import execute from "../execute";
import Storage from "../storage"

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

    expect(database.tables['t'].rows.length).toBe(1);
    expect(database.tables['t'].rows[0].numCells()).toBe(1);
    expect(database.tables['t'].rows[0].cell(0).getData()).toBe(20);
  })

  test('INSERT with multiple values', () => {
    let {storage} = execute(`
      CREATE Table t (
        age INT
      );

      INSERT INTO t VALUES (21), (22)
    `);

    let database = storage.databases['default'];

    expect(database.tables['t'].rows.length).toBe(2);
    expect(database.tables['t'].rows[0].cell(0).getData()).toBe(21);
    expect(database.tables['t'].rows[1].cell(0).getData()).toBe(22);
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
    expect(database.tables['l'].columns.length).toBe(2);
    expect(database.tables['l'].columns[0]).toBe('name');
    expect(database.tables['l'].columns[1]).toBe('age');

    expect(database.tables['l'].rows.length).toBe(2);

    // First row
    expect(database.tables['l'].rows[0].cell(0).getData()).toBe('Tim');
    expect(database.tables['l'].rows[0].cell(1).getData()).toBe(30);

    // Second row
    expect(database.tables['l'].rows[1].cell(0).getData()).toBe('Liz');
    expect(database.tables['l'].rows[1].cell(1).getData()).toBe(21);
  });

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
})