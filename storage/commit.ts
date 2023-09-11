export type Commit = number;

let latestCommit = 0;
export function newCommit() {
  latestCommit = latestCommit += 1;
  return latestCommit;
}

export function getLatestCommit() {
  return latestCommit;
}

export class Committed {
  objectNameForErrorMessages:string;

  createdAt:Commit;
  deletedAt:Commit|null = null;

  constructor(objectNameForErrormessages:string, commit:Commit = newCommit()) {
    this.objectNameForErrorMessages = objectNameForErrormessages;
    this.createdAt = commit;
  }

  isDeleted(commit?:Commit) {
    // If it hasn't been deleted at all, always return false. 
    if (this.deletedAt == null) {
      return false;
    }

    // deletedAt must be set. 
    // If no commit was asked for, then we're looking for the
    // latest state, which is deleted. 
    if (typeof commit == "undefined") {
      return true;
    }

    // Else we have a commit; only return true if commit is 
    // greater than the deleted time. 
    return commit > this.deletedAt;
  }

  delete(commit?:Commit) {
    if (this.isDeleted()) {
      throw new Error("Cannot delete " + this.objectNameForErrorMessages + "; It's already deleted!");
    }

    this.deletedAt = typeof commit != "undefined" ? commit : newCommit();
  }
}