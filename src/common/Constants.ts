import { Magic, Language, Kind } from "./Types";

export const notebookHeader = "# Databricks notebook source";
export const cellSeparator = "\n\n# COMMAND ----------\n\n";
export const cellSeparatorRegex = "\n\n# COMMAND -*[^\n]\n\n";

export const magicHeader = "# MAGIC";
export const mdMagicHeader = `${magicHeader} %${Magic.md}`;

export const magicMap = new Map([
  [Magic.md, { language: Language.md, kind: Kind.markdown }],
  [Magic.sql, { language: Language.sql, kind: Kind.code }],
  [Magic.run, { language: Language.run, kind: Kind.code }],
  [Magic.sh, { language: Language.sh, kind: Kind.code }],
  [Magic.pip, { language: Language.sh, kind: Kind.code }],
  [Magic.fs, { language: Language.fs, kind: Kind.code }],
  [Magic.none, { language: Language.python, kind: Kind.code }],
  [Magic.invalid, { language: Language.unknown, kind: Kind.code }],
]);

export const languageMap = new Map([
  [Language.md, Magic.md],
  [Language.sql, Magic.sql],
  [Language.run, Magic.run],
  [Language.sh, Magic.sh],
  [Language.remoteSh, Magic.sh],
  [Language.fs, Magic.fs],
]);

export const controllerId = "db-notebook-controller-id";
export const notebookType = "db-notebook";
export const controllerLabel = "Databricks Notebook Controller";
