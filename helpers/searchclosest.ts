// Modified from here to always return the closest *smaller* value than the target
// https://stackoverflow.com/questions/48875912/binarysearch-to-find-closest-number-to-target-undefined-as-return-value
export default function searchClosest(arr:Array<number>, target:number, lo:number = 0, hi:number = arr.length - 1):number {
  if (target < arr[lo]) {return arr[0]}
  if (target > arr[hi]) {return arr[hi]}
  if (hi - lo < 2) {return arr[lo]};

  const mid = Math.floor((hi + lo) / 2);

  return target < arr[mid]
    ? searchClosest(arr, target, lo, mid)
    : target > arr[mid] 
      ? searchClosest(arr, target, mid, hi)
      : arr[mid]
}