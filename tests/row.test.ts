import Row, { JoinedRow } from "../storage/row";

describe("JoinedRow", () => {
  test("correctly manages indexes", () => {
    // This one was a doozy. Having rows of different sizes exposed a really
    // hard to find (but dumb) issue. Keeping this way just to be safe.
    let rowOne = new Row(["a", "b", "c"]);
    let rowTwo = new Row(["d", "e"]);

    let joinedRow = new JoinedRow(rowOne, rowTwo); 

    expect(joinedRow.numCells()).toBe(5);

    // All values for good measure
    expect(joinedRow.cell(0).getData()).toBe("a");
    expect(joinedRow.cell(1).getData()).toBe("b");
    expect(joinedRow.cell(2).getData()).toBe("c");
    expect(joinedRow.cell(3).getData()).toBe("d");
    expect(joinedRow.cell(4).getData()).toBe("e");

    // Now all together
    expect(joinedRow.getData()).toEqual(["a", "b", "c", "d", "e"])
  })

  test("correctly manages indexes 2", () => {
    // Same as above. Keeping an equal amount of rows test just to be safe. 
    let rowOne = new Row(["a", "b", "c"]);
    let rowTwo = new Row(["d", "e", "f"]);

    let joinedRow = new JoinedRow(rowOne, rowTwo); 

    expect(joinedRow.numCells()).toBe(6);

    // All values for good measure
    expect(joinedRow.cell(0).getData()).toBe("a");
    expect(joinedRow.cell(1).getData()).toBe("b");
    expect(joinedRow.cell(2).getData()).toBe("c");
    expect(joinedRow.cell(3).getData()).toBe("d");
    expect(joinedRow.cell(4).getData()).toBe("e");
    expect(joinedRow.cell(5).getData()).toBe("f");

    // Now all together
    expect(joinedRow.getData()).toEqual(["a", "b", "c", "d", "e", "f"])
  })
})