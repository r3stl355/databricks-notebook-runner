import { TextDecoder, TextEncoder } from "util";
import * as vscode from "vscode";
import { Kind, Language, Magic, RawNotebookCell } from "../common/Types";
import * as c from "../common/Constants";
import * as u from "../common/Utils";

export class DatabricksNotebookSerializer implements vscode.NotebookSerializer {
  async deserializeNotebook(
    content: Uint8Array,
    _token: vscode.CancellationToken
  ): Promise<vscode.NotebookData> {
    var contents = new TextDecoder().decode(content);

    let raw: RawNotebookCell[];
    try {
      raw = <RawNotebookCell[]>this.parseNotebook(contents);
    } catch (error) {
      vscode.window.showErrorMessage(`Error parsing notebook: ${error}`);
      raw = [];
    }

    const cells = raw.map(
      (c) =>
        new vscode.NotebookCellData(
          c.kind === Kind.markdown
            ? vscode.NotebookCellKind.Markup
            : vscode.NotebookCellKind.Code,
          c.value,
          c.language
        )
    );

    return new vscode.NotebookData(cells);
  }

  async serializeNotebook(
    data: vscode.NotebookData,
    _token: vscode.CancellationToken
  ): Promise<Uint8Array> {
    let contents = data.cells
      .map((cell) => this.serializeCell(cell))
      .join(`${c.cellSeparator}`);

    return new TextEncoder().encode(c.notebookHeader + "\n" + contents);
  }

  private parseNotebook(contents: string): RawNotebookCell[] {
    if (contents.startsWith(c.notebookHeader)) {
      const headerPattern = new RegExp(c.notebookHeader, "g");
      const withoutHeader = contents.split(headerPattern)[1].trim();
      const cellSeparatorPattern = new RegExp(c.cellSeparatorRegex, "g");
      const cells = withoutHeader.split(cellSeparatorPattern);
      return cells.map((v) => this.parseCell(v));
    } else {
      return [u.pythonRowNotebookCell(contents)];
    }
  }

  private parseCell(contents: string): RawNotebookCell {
    let res = u.parseCellText(contents);
    if (res.kind === Kind.markdown) {
      // Remove `%md` from the markdown cell, anything before the `%md` row is irrelevant
      res.value = res.value
        .trimStart()
        .replace(`\%${Magic.md}`, "")
        .trimStart();
    }
    return res;
  }

  private serializeCell(cell: vscode.NotebookCellData): string {
    // Important design decitions
    //  - magic in the current cell content has a priority over the cell properties
    //  - if there is no magic then honor then it can be Python or Markdown only
    //  - in which case current cell language will determine if it is a Markdown
    let cellText = cell.value;
    const rawCell = u.parseCellText(cellText, false);
    if (rawCell.magic === Magic.none) {
      if (cell.languageId === Language.python) {
        // This is surely a Python cell
        return cell.value;
      } else if (cell.languageId === Language.md) {
        // Insert the relevant magic into cell
        // const language = enumFromString(Language, cell.languageId)!;
        // const magic = languageMap.get(language);
        cellText = `%${Magic.md}\n${cellText}`;
      }
    }
    const split = cellText.split(/\n/gm);
    let res = `${c.magicHeader} ${split.join(`\n${c.magicHeader} `)}`;
    return res;
  }
}
