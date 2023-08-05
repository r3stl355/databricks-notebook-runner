import { tmpdir } from "os";
import { join } from "path";
import { randomBytes } from "crypto";
import { Kind, Language, Magic, RawNotebookCell } from "./Types";
import * as c from "./Constants";

export const randomName = () => randomBytes(16).toString("hex");
export const isString = (obj: any) => typeof obj === "string";
export const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

export const pythonRowNotebookCell = (cellText: string) => {
  return {
    magic: Magic.none,
    kind: Kind.code,
    value: cellText,
    language: Language.python,
  };
};

export function parseCellText(
  cellText: string,
  canHaveMagicHeader: boolean = true
): RawNotebookCell {
  const trimmed = cellText.trimStart();
  if (
    (canHaveMagicHeader && !trimmed.startsWith(c.magicHeader)) ||
    (!canHaveMagicHeader && !trimmed.startsWith("%"))
  ) {
    return pythonRowNotebookCell(cellText);
  }
  const headerRegStr = canHaveMagicHeader ? c.magicHeader + " " : "";
  const regStr = `^${headerRegStr}\%(?<magic>[^\\s\\n]*)`;
  const magicTypePatern = RegExp(regStr, "i");
  if (!magicTypePatern.test(trimmed)) {
    const magic = Magic.invalid;
    const { language, kind } = c.magicMap.get(magic)!;
    return {
      magic: magic,
      language: language,
      value: cellText,
      kind: kind,
    };
  }
  const magicStr =
    magicTypePatern.exec(trimmed)?.groups?.magic?.toLowerCase() ?? "invalid"!;
  let magic = enumFromString(Magic, magicStr) ?? Magic.invalid;
  let { language, kind } = c.magicMap.get(magic)!;
  return {
    magic: magic,
    language: language,
    value: canHaveMagicHeader ? removeMagicHeaders(cellText) : cellText,
    kind: kind,
  };
}

export function enumFromString<T>(
  enm: { [s: string]: T },
  value: string
): T | undefined {
  return (Object.values(enm) as unknown as string[]).includes(value)
    ? (value as unknown as T)
    : undefined;
}

export const removeMagicHeaders = (contents: string) => {
  const magicPattern = new RegExp(`${c.magicHeader}\\s?`, "gm");
  const cleaned = contents.replace(magicPattern, "");
  return cleaned;
};

export function getTmpFilePath(dir: string, namePrefix: string, ext: string) {
  if (!isString(dir)) {
    dir = tmpdir();
  }
  return join(dir, `${namePrefix}${randomName()}.${ext}`);
}
