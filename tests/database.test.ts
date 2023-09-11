import execute from "../execute"

describe("Database", () => {
  test("tables are only returned if they existed at the time of commit", () => {
    // We're gonna use execute(), because it's easier. 

    let result = execute(`
      CREATE TABLE First (
        age INT
      );

      CREATE TABLE Second (
        name VARCHAR(20)
      )
    `);

    expect(result.results.length).toBe(2);

    let firstCommit = result.results[0] as number;
    let secondCommit = result.results[1] as number;

    let storage = result.storage;
    let database = storage.getDatabase('default');

    // Happy path (table exists now)
    expect(database.hasTable('First')).toBe(true);
    expect(database.hasTable('Second')).toBe(true);

    // Now be explicit about First existing at the First commit, just for good measure. 
    expect(database.hasTable('First', firstCommit)).toBe(true);

    // Now test to make sure the Second table didn't exist in the First commit
    expect(database.hasTable('Second', firstCommit)).toBe(false);
    expect(database.hasTable('Second', secondCommit)).toBe(true);
  })
})