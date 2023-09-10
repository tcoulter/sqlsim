import searchClosest from "../helpers/searchclosest";
import { Commit, newCommit } from "./commit";

export type CellData = number|String|null;

export default class Cell {
  data:Record<number, CellData> = {};
  latestCommit:Commit;

  constructor(initialValue:CellData, initialCommit:Commit = newCommit()) {
    this.data[initialCommit] = initialValue;
    this.latestCommit = initialCommit;
  }

  put(value:CellData, commit?:Commit) {
    if (typeof commit == "undefined") {
      this.latestCommit = newCommit();
      this.data[this.latestCommit] = value;
    } else {
      if (commit > this.latestCommit) {
        this.latestCommit = commit;
      }

      this.data[commit] = value;
    }
  }

  getData(commit?:Commit):CellData {
    // If no commit was passed, return the latest.
    if (typeof commit == "undefined") {
      return this.data[this.latestCommit];
    }

    // If a specific commit was passed, return it if we have it. 
    if (typeof this.data[commit] != "undefined") {
      return this.data[commit];
    }

    // If we don't have that exact commit, the one that's closest
    // that existed before the requested commit. 

    // From here, to ensure that we get number types for numbered keys
    // https://stackoverflow.com/questions/52856496/typescript-object-keys-return-string
    const getKeys = Object.keys as <T extends object>(obj: T) => Array<keyof T>

    // Here', "commit on file" means the commit that exists when this commit occured, 
    // since there's no explicit entry at the requested commit time. 
    // TODO: Might be able to get away with not sorting if the input is sorted. 
    let commits = getKeys(this.data).sort();
    let commitOnFile = searchClosest(commits, commit);

    return this.data[commitOnFile];
  }
}