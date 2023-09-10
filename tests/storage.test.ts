import Storage from "../storage";

describe("Storage", () => {
  test("hasDatabase() reports false on initialization", () => {
    let storage = new Storage();

    expect(storage.hasDatabase("abcd")).toBe(false);
  })
})