import Storage from "../storage";

describe("Storage", () => {
  test("hasDatabase() has single default database on initialization", () => {
    let storage = new Storage();

    expect(storage.hasDatabase("default")).toBe(true);
    expect(storage.hasDatabase("asdf")).toBe(false);
  });
})