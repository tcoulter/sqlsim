import searchClosest from "../helpers/searchclosest";
import { Commit, newCommit } from "./commit";

export type CellData = number|string|null|boolean;

export default class Cell {
  data:Map<number, CellData>;
  latestCommit:Commit;

  constructor(initialValue:CellData, initialCommit:Commit = newCommit()) {
    this.data = new Map();
    this.data.set(initialCommit, initialValue);
    this.latestCommit = initialCommit;
  }

  put(value:CellData, commit?:Commit) {
    if (typeof commit == "undefined") {
      this.latestCommit = newCommit();
      this.data.set(this.latestCommit, value);
    } else {
      if (commit > this.latestCommit) {
        this.latestCommit = commit;
      }

      this.data.set(commit, value);
    }
  }

  getData(commit?:Commit):CellData {
    // If no commit was passed, return the latest.
    if (typeof commit == "undefined") {
      return this.data.get(this.latestCommit) as CellData; // TODO: Technically this can return undefined.
    }

    // If a specific commit was passed, return it if we have it. 
    if (typeof this.data.get(commit) != "undefined") {
      return this.data.get(commit) as CellData;
    }

    // If we don't have that exact commit, the one that's closest
    // that existed before the requested commit. 

    // Here', "commit on file" means the commit that exists when this commit occured, 
    // since there's no explicit entry at the requested commit time. 
    // TODO: Might be able to get away with not sorting if the input is sorted. 
    let commits = Array.from(this.data.keys());

    let commitOnFile = searchClosest(commits, commit);

    return this.data.get(commitOnFile) as CellData;
  }
}