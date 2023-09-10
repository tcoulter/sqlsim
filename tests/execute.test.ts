import execute from "../execute";
import Storage from "../storage"

describe("execute()", () => {
  // We're sharing storage here. All test will rely on each other
  // in a specific order. Think of it like running SQL statemnets
  // in sequence. 
  let storage = new Storage();

  test("simple CREATE TABLE", () => {
    execute(`
      CREATE Table t (
        age INT
      )
    `, storage);

    expect(Object.keys(storage.databases).length).toBe(1);

    let database = storage.databases['default'];

    expect(database.hasTable('t')).toBe(true)
    expect(database.tables['t'].columns.length).toBe(1);
    expect(database.tables['t'].columns[0]).toBe('age');
  }); 

  test('simple INSERT INTO', () => {
    execute(`
      INSERT INTO t VALUES (20)
    `, storage);

    let database = storage.databases['default'];

    expect(database.tables['t'].rows.length).toBe(1);
    expect(database.tables['t'].rows[0].cells.length).toBe(1);
    expect(database.tables['t'].rows[0].cells[0].get()).toBe(20);
  })

  test('INSERT with multiple values', () => {
    execute(`
      INSERT INTO t VALUES (21), (22)
    `, storage);

    let database = storage.databases['default'];

    // Remember, this tes relys on the ones before it, so if we add 
    // two items, there should be three rows. 
    expect(database.tables['t'].rows.length).toBe(3);
    expect(database.tables['t'].rows[1].cells[0].get()).toBe(21);
    expect(database.tables['t'].rows[2].cells[0].get()).toBe(22);
  })

  test('CREATE and INSERT multiple columns, multiple values', () => {
    execute(`
      CREATE TABLE l (
        name VARCHAR(20),
        age INT
      );

      INSERT INTO l VALUES ('Tim', 30), ('Liz', 21);
    `, storage);

    let database = storage.databases['default'];

    expect(database.hasTable('l')).toBe(true);
    expect(database.tables['l'].columns.length).toBe(2);
    expect(database.tables['l'].columns[0]).toBe('name');
    expect(database.tables['l'].columns[1]).toBe('age');

    expect(database.tables['l'].rows.length).toBe(2);

    // First row
    expect(database.tables['l'].rows[0].cells[0].get()).toBe('Tim');
    expect(database.tables['l'].rows[0].cells[1].get()).toBe(30);

    // Second row
    expect(database.tables['l'].rows[1].cells[0].get()).toBe('Liz');
    expect(database.tables['l'].rows[1].cells[1].get()).toBe(21);
  })
})