import { NotebookCellKind } from "vscode";

export enum Language {
  unknown = "plaintext",
  python = "python",
  sql = "sql",
  run = "run",
  sh = "shellscript",
  remoteSh = "remote-sh",
  md = "markdown",
  fs = "fs",
}

export enum Kind {
  markdown = NotebookCellKind.Markup,
  code = NotebookCellKind.Code,
  unknown = -1,
}

export enum Magic {
  invalid = "invalid",
  none = "",
  md = "md",
  sh = "sh",
  run = "run",
  sql = "sql",
  fs = "fs",
  pip = "pip",
}

export interface RawNotebookCell {
  magic: Magic;
  language: Language;
  value: string;
  kind: Kind;
}
