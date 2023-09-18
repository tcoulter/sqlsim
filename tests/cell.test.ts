import Cell from "../storage/cell"
import { getLatestCommit } from "../storage/commit";

describe("Cell", () => {
  test("cell can hold data at multiple commits", () => {
    let startingCommit = getLatestCommit();

    let expectedCommits = [
      startingCommit + 1,
      startingCommit + 2,
      startingCommit + 3,
      startingCommit + 4, 
      startingCommit + 5 
    ]

    let cell = new Cell(11);

    cell.put(12);
    cell.put(13);
    cell.put(14);
    cell.put(15); 

    // Note that keys come back as strings
    expect(Array.from(cell.data.keys())).toEqual(expectedCommits);

    // Expect commits to have the right commit number and value
    expect(cell.data.get(expectedCommits[0])).toBe(11);
    expect(cell.data.get(expectedCommits[1])).toBe(12);
    expect(cell.data.get(expectedCommits[2])).toBe(13);
    expect(cell.data.get(expectedCommits[3])).toBe(14);
    expect(cell.data.get(expectedCommits[4])).toBe(15);
  });

  test("commit management occurs across cells", () => {
    let startingCommit = getLatestCommit();

    let cellOne = new Cell(10); // first commit
    let cellTwo = new Cell(20); // second commit

    cellOne.put(11); // third commit
    cellTwo.put(21); // fourth commit

    expect(Array.from(cellOne.data.keys())).toEqual([
      startingCommit + 1,
      startingCommit + 3
    ]);
    expect(Array.from(cellTwo.data.keys())).toEqual([
      startingCommit + 2, 
      startingCommit + 4
    ]);
  });

  test("get() gets the value at the latest commit", () => {
    let cell = new Cell(11);

    cell.put(12);
    cell.put(13);
    cell.put(14);
    cell.put(15);

    expect(cell.getData()).toBe(15);
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

    // This creation/commit bumps the commit index, making it so
    // cell one will never have this commit. 
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
    expect(cellOne.getData(searchCommit)).toBe(cellOne.getData(expectedCommit));
    expect(cellOne.getData(searchCommit)).toBe(expectedValue);
  });

  test("cell commit keys are treated as numbers", () => {
    // This test likely doesn't test anything now. But! There was a terrible issue
    // earlier where commits were being coerced into strings, causing 102 to be 
    // less than 99 when sorted (e.g., "102" < "99").

    let cell = new Cell(10, 99);
    cell.put(20, 102);

    expect(cell.getData()).toEqual(20);
  })
})