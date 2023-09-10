export type Commit = number;

let latestCommit = 0;
export function newCommit() {
  latestCommit = latestCommit += 1;
  return latestCommit;
}

export function getLatestCommit() {
  return latestCommit;
}