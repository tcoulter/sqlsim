import searchClosest from "../helpers/searchclosest";

describe('searchclosest()', () => {
  test('will return the closest values *less than* the target given', () => {
    
    let input = [1, 23, 45, 67, 94, 122];

    expect(searchClosest(input, 96)).toBe(94);
    expect(searchClosest(input, 44)).toBe(23);
    expect(searchClosest(input, 207)).toBe(122);
    expect(searchClosest(input, 0)).toBe(1);

    input = [1, 3];

    expect(searchClosest(input, 2)).toBe(1);

    input = [1, 2, 3];

    expect(searchClosest(input, 2)).toBe(2);
  });
});