import Cell from "../storage/cell"

describe("Cell", () => {
  test("cell can hold data at multiple commits", () => {
    let cell = new Cell(11);

    cell.put(12);
    cell.put(13);
    cell.put(14);
    cell.put(15);

    // Note that keys come back as strings
    expect(Object.keys(cell.data)).toEqual(["1", "2", "3", "4", "5"]);

    // Expect commits to have the right commit number and value
    expect(cell.data[1]).toBe(11);
    expect(cell.data[2]).toBe(12);
    expect(cell.data[3]).toBe(13);
    expect(cell.data[4]).toBe(14);
    expect(cell.data[5]).toBe(15);
  });

  test("commit management occurs across cells", () => {
    let cellOne = new Cell(10);
    let cellTwo = new Cell(20);

    cellOne.put(11);
    cellTwo.put(21);

    // Note: These commits depend on the previous test
    expect(Object.keys(cellOne.data)).toEqual(["6", "8"]);
    expect(Object.keys(cellTwo.data)).toEqual(["7", "9"]);
  });

  test("get() gets the value at the latest commit", () => {
    let cell = new Cell(11);

    cell.put(12);
    cell.put(13);
    cell.put(14);
    cell.put(15);

    expect(cell.get()).toBe(15);
  });

  test("searching at a commit that doesn't exist", () => {
    // Create a new cell and add data twice, the second value will
    // be the value we eventually want returned. 
    let cellOne = new Cell(10);
    let expectedValue = 11;
    cellOne.put(expectedValue);

    // Record the latest commit: This is the commit that 
    // we expect will be returned. 
    let expectedCommit = cellOne.latestCommit;

    let cellTwo = new Cell(20);

    // Note that the latest commit on cellTwo is the commit
    // cellOne missed out on. That's hte one we'll search for
    let searchCommit = cellTwo.latestCommit;

    // Create a couple new commits on cellOne to make sure there's data to search
    cellOne.put(12);
    cellOne.put(13);

    // Make sure the two commits don't equal each other, just for good measure
    expect(searchCommit).not.toBe(expectedCommit);

    // Test that getting at the expected commit matches the value,
    // all while searching for a commit that doesn't exist in cellOne's data.
    expect(cellOne.get(searchCommit)).toBe(cellOne.get(expectedCommit));
    expect(cellOne.get(searchCommit)).toBe(expectedValue);
  });
})