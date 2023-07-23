import { TextDecoder, TextEncoder } from "util";
import * as vscode from "vscode";
import { readFile } from "fs/promises";
import { existsSync } from "fs";
import { randomBytes } from "crypto";
// import { Terminal } from "vscode";
import * as path from "path";
import { tmpdir } from "os";

const notebookHeader = "# Databricks notebook source";
const cellSeparator = "\n\n# COMMAND ----------\n\n";
const cellSeparatorRegex = "\n\n# COMMAND -*[^\n]\n\n";

enum Language {
  unknown = "plaintext",
  python = "python",
  sql = "sql",
  run = "run",
  sh = "shellscript",
  remoteSh = "remote-sh",
  md = "markdown",
  fs = "fs",
}

enum Kind {
  markdown = vscode.NotebookCellKind.Markup,
  code = vscode.NotebookCellKind.Code,
  unknown = -1,
}

enum Magic {
  invalid = "invalid",
  none = "",
  md = "md",
  sh = "sh",
  run = "run",
  sql = "sql",
  fs = "fs",
}

interface RawNotebookCell {
  magic: Magic;
  language: Language;
  value: string;
  kind: Kind;
}

const magicHeader = "# MAGIC";
const mdMagicHeader = `${magicHeader} %${Magic.md}`;

const magicMap = new Map([
  [Magic.md, { language: Language.md, kind: Kind.markdown }],
  [Magic.sql, { language: Language.sql, kind: Kind.code }],
  [Magic.run, { language: Language.run, kind: Kind.code }],
  [Magic.sh, { language: Language.sh, kind: Kind.code }],
  [Magic.fs, { language: Language.fs, kind: Kind.code }],
  [Magic.none, { language: Language.python, kind: Kind.code }],
  [Magic.invalid, { language: Language.unknown, kind: Kind.code }],
]);

const languageMap = new Map([
  [Language.md, Magic.md],
  [Language.sql, Magic.sql],
  [Language.run, Magic.run],
  [Language.sh, Magic.sh],
  [Language.remoteSh, Magic.sh],
  [Language.fs, Magic.fs],
  // [Language.python, Magic.none]
  // [Language.python, kind: Kind.code }],
  // [Magic.invalid, { language: Language.unknown, kind: Kind.code }],
]);

function enumFromString<T>(
  enm: { [s: string]: T },
  value: string
): T | undefined {
  return (Object.values(enm) as unknown as string[]).includes(value)
    ? (value as unknown as T)
    : undefined;
}

const pythonRowNotebookCell = (cellText: string) => {
  return {
    magic: Magic.none,
    kind: Kind.code,
    value: cellText,
    language: Language.python,
  };
};

function parseCellText(
  cellText: string,
  canHaveMagicHeader: boolean = true
): RawNotebookCell {
  const trimmed = cellText.trimStart();
  if (
    (canHaveMagicHeader && !trimmed.startsWith(magicHeader)) ||
    (!canHaveMagicHeader && !trimmed.startsWith("%"))
  ) {
    return pythonRowNotebookCell(cellText);
  }
  const headerRegStr = canHaveMagicHeader ? magicHeader + " " : "";
  const regStr = `^${headerRegStr}\%(?<magic>[^\\s\\n]*)`;
  const magicTypePatern = RegExp(regStr, "i");
  if (!magicTypePatern.test(trimmed)) {
    const magic = Magic.invalid;
    const { language, kind } = magicMap.get(magic)!;
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
  let { language, kind } = magicMap.get(magic)!;
  return {
    magic: magic,
    language: language,
    value: canHaveMagicHeader ? removeMagicHeaders(cellText) : cellText,
    kind: kind,
  };
}

const removeMagicHeaders = (contents: string) => {
  const magicPattern = new RegExp(`${magicHeader}\\s?`, "gm");
  const cleaned = contents.replace(magicPattern, "");
  return cleaned;
  // if (res.kind === Kind.markdown) {
  //   // Remove `%md` from the markdown cell, anything before the `%md` row is irrelevant
  //   res.value = res.value
  //     .trimStart()
  //     .replace(`\%${Magic.md}`, "")
  //     .trimStart();
  // }
};

const randomName = () => randomBytes(16).toString("hex");
const isString = (obj: any) => typeof obj === "string";
const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));
// const delaySync = (ms: number) => delay(ms).then((v) => v);
// Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

const logDir = `${tmpdir()}/db-notebook`;
const cmdOutput = getTmpFilePath(logDir, "cell-command", "out");

const pythonReplName = "Databricks Notebook (Python)";

const pythonReplInitCommand = `
from importlib import reload
import os, sys
import subprocess
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language

sys.path.append("${logDir}")

os.makedirs("${logDir}", exist_ok=True)

class my_logger:
  def __init__(self, log_file):
    self.log_file = log_file
    if os.path.exists(log_file):
      os.remove(log_file)
  def write(self, msg):
    with open(self.log_file, "a") as f:
      f.write(msg)
  def clear_done(self, done_file):
    if os.path.exists(done_file):
      os.remove(done_file)
  def flag_done(self, done_file):
    with open(done_file, "w") as f:
      f.write("DONE")
  def flush(self):
    pass

logger = my_logger("${cmdOutput}")

spark = None

try:
  from databricks.connect import DatabricksSession
  spark = DatabricksSession.builder.getOrCreate()
except e:
  print(f"Unable to create a remote Spark Session: {e}")

sys.stdout = logger
sys.stderr = logger

w = WorkspaceClient()
lang = Language.python
`;

function getTmpFilePath(dir: string, namePrefix: string, ext: string) {
  if (!isString(dir)) {
    dir = tmpdir();
  }
  return path.join(dir, `${namePrefix}${randomName()}.${ext}`);
}

let controller: any = null;

export function activate(context: vscode.ExtensionContext) {
  // if (
  //   vscode.workspace.workspaceFolders === undefined ||
  //   vscode.workspace.workspaceFolders?.length === 0
  // ) {
  //   vscode.window.showErrorMessage("Open a folder to use Databricks extension");
  //   return undefined;
  // }

  // Serializer
  const serializer = new DatabricksNotebookSerializer();
  context.subscriptions.push(
    vscode.workspace.registerNotebookSerializer(
      "databricks-notebook",
      serializer
    )
  );

  // Controller
  controller = new Controller();

  context.subscriptions.push(controller);
  controller
    .init()
    .then(() =>
      vscode.window.showInformationMessage(
        "Databricks Notebook runner is active!"
      )
    );

  vscode.window.onDidCloseTerminal(function (event) {
    if (event.name === pythonReplName && controller !== null) {
      controller.removePythonRepl();
    }
  });

  // vscode.workspace.onDidChangeNotebookDocument(function (event) {
  //   controller.onDidChangeNotebookDocument(event);
  // });
}

class DatabricksNotebookSerializer implements vscode.NotebookSerializer {
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
      .join(`${cellSeparator}`);

    return new TextEncoder().encode(notebookHeader + "\n" + contents);
  }

  // onDidChangeNotebookDocument(event: vscode.NotebookDocumentChangeEvent) {
  //   const changes = event.contentChanges
  //     .filter((v) => v.addedCells?.length > 0)
  //     .map((v) => v.addedCells)
  //     .flatMap((v) => v)
  //     .map((v) => v.metadata.languageId);
  // }

  private parseNotebook(contents: string): RawNotebookCell[] {
    if (contents.startsWith(notebookHeader)) {
      const headerPattern = new RegExp(notebookHeader, "g");
      const withoutHeader = contents.split(headerPattern)[1].trim();
      const cellSeparatorPattern = new RegExp(cellSeparatorRegex, "g");
      const cells = withoutHeader.split(cellSeparatorPattern);
      return cells.map((v) => this.parseCell(v));
    } else {
      return [pythonRowNotebookCell(contents)];
    }
  }

  private parseCell(contents: string): RawNotebookCell {
    let res = parseCellText(contents);
    // if (res.magic === Magic.invalid) {
    //   vscode.window.showErrorMessage(`Magic ${res.magic} not supported`);
    // }
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
    const rawCell = parseCellText(cellText, false);
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
    // } if (cell.kind === vscode.NotebookCellKind.Markup) {
    //   // Because we are not using the magic in Markdown cells, handle them separately
    //   res = `${mdMagicHeader}\n${res}`;
    // }
    const split = cellText.split(/\n/gm);
    let res = `${magicHeader} ${split.join(`\n${magicHeader} `)}`;
    return res;
  }
}

class Controller {
  readonly controllerId = "databricks-notebook-controller-id";
  readonly notebookType = "databricks-notebook";
  readonly label = "Databricks Notebook";
  readonly supportedLanguages = [
    Language.python,
    Language.sql,
    Language.sh,
    Language.remoteSh,
    Language.run,
    Language.fs,
  ];

  private readonly controller: vscode.NotebookController;
  private pythonRepl: any;
  private initialised: boolean = false;
  private _executionOrder = 0;
  private interrupted = false;

  constructor() {
    this.controller = vscode.notebooks.createNotebookController(
      this.controllerId,
      this.notebookType,
      this.label
    );

    this.controller.supportedLanguages = this.supportedLanguages;
    this.controller.supportsExecutionOrder = true;
    this.controller.executeHandler = this.execute.bind(this);
    this.controller.interruptHandler = this.interrupt.bind(this);
  }

  dispose(): void {
    this.controller.dispose();
  }

  async init() {
    this.pythonRepl = await this.createPythonRepl();
    this.initialised = true;
  }

  private async execute(
    cells: vscode.NotebookCell[],
    _notebook: vscode.NotebookDocument,
    _controller: vscode.NotebookController
  ): Promise<void> {
    this.interrupted = false;
    while (!this.initialised && !this.interrupted) {
      await delay(100);
    }
    if (this.interrupted) {
      return;
    }
    for (let cell of cells) {
      await this.executeCell(cell);
    }
  }

  private async executeCell(cell: vscode.NotebookCell): Promise<void> {
    let cellText = cell.document.getText().trim();

    if (cellText.length === 0) {
      return;
    }
    // Important design decition - current cell content takes over the cell properties.
    // This way the cell type can be changed and enforced via magic strings
    const rawCell = parseCellText(cellText, false);

    // Some magic is used by more than one language (e.g. `sh`)
    let language = rawCell.language;
    if (
      rawCell.magic === Magic.sh &&
      cell.document.languageId !== rawCell.language &&
      cell.document.languageId === Language.remoteSh
    ) {
      language = Language.remoteSh;
    }
    const command = this.prepareCommand(cellText, language);
    if (command.length > 0) {
      await this.executeCommand(
        cell,
        enumFromString(Language, language)!,
        command
      );
    }
  }

  private async executeCommand(
    cell: vscode.NotebookCell,
    language: Language,
    command: string
  ): Promise<void> {
    const execution = this.controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this._executionOrder;
    execution.start(Date.now());

    let cmdId = randomName();
    let flagFile = `${logDir}/flag-${cmdId}`;
    let markerStart = "-- db-notebook-";
    let marker = `${markerStart}${cmdId} -->`;

    this.runPythonCommand(command, flagFile, marker);

    const statusCheckDelay = 100;
    while (!existsSync(flagFile) && !this.interrupted) {
      await delay(statusCheckDelay);
    }

    let res = "OK";
    if (this.interrupted) {
      res = "Cancelled";
    } else if (existsSync(cmdOutput)) {
      let output = new TextDecoder().decode(await readFile(cmdOutput));
      if (output.match(marker)) {
        res = output.split(marker)[1].split(markerStart)[0];
        if (language === Language.run) {
          // Filter out the response from reload
          res = res
            .split(/\n/gm)
            .filter((v) => !(v.startsWith("<module ") && v.endsWith(">")))
            .join("\n");
        } else if (language === Language.sh || language === Language.remoteSh) {
          res = res.replace(/\\n/gm, "\n").replace(/'/gm, "");
        } else if (language === Language.fs) {
          res = res.replace(/, FileInfo/gm, "\nFileInfo").replace(/'/gm, "");
        }
      }
      if (res.length === 0) {
        res = "OK";
      }
    }

    execution.replaceOutput([
      new vscode.NotebookCellOutput([vscode.NotebookCellOutputItem.text(res)]),
    ]);
    execution.end(true, Date.now());
  }

  private prepareCommand(cellText: string, language: Language): string {
    if (language === Language.sql) {
      return this.prepareSqlCommand(cellText);
    } else if (language === Language.run) {
      return this.prepareRunCommand(cellText);
    } else if (language === Language.sh) {
      return this.prepareShCommand(cellText);
    } else if (language === Language.remoteSh) {
      return this.prepareRemoteShCommand(cellText);
    } else if (language === Language.fs) {
      return this.prepareFsCommand(cellText);
    }
    return this.preparePythonCommand(cellText);
  }

  private splitStripEmpty(txt: string): string[] {
    return txt
      .trim()
      .split(/\n/gm)
      .map((l) => l.trim())
      .filter((v) => v.length > 0);
  }

  private preparePythonCommand(cellText: string): string {
    return cellText.replace(/dbutils\./gim, "w.dbutils.");
  }

  private prepareShCommand(cellText: string): string {
    return this.splitStripEmpty(cellText.replace("%sh", ""))
      .map(
        (v) =>
          `subprocess.run(['${v.replace(
            " ",
            "','"
          )}'], capture_output=True).stdout.decode()`
      )
      .join("\n");
  }

  private prepareRemoteShCommand(cellText: string): string {
    let command = "w.clusters.ensure_cluster_is_running(w.config.cluster_id)\n";
    command += `c = w.command_execution\n`;
    command += `c_id = c.create_and_wait(cluster_id=w.config.cluster_id, language=lang).id\n`;
    let subCmd = this.splitStripEmpty(cellText.replace("%sh", ""))
      .map(
        (v) =>
          `import subprocess\\nsubprocess.run(['${v.replace(
            " ",
            "','"
          )}'], capture_output=True).stdout.decode()`
      )
      .map(
        (v) =>
          `print(c.execute_and_wait(context_id=c_id, cluster_id=w.config.cluster_id, language=lang, command="${v}").results.data)`
      )
      .join("\n");
    command += subCmd;
    return command;
  }

  private prepareSqlCommand(cellText: string): string {
    let command = cellText.replace("%sql", "").trim();
    if (command.length > 0) {
      command = `spark.sql("${command.replace(/\n/gm, " ")}").show()`;
    }
    return command;
  }

  private prepareRunCommand(cellText: string): string {
    let notebookPath = vscode.window.activeTextEditor?.document.uri.path!;
    if (isString(notebookPath)) {
      let cmd = cellText.replace("%run ", "").trim();
      let dir = path.dirname(notebookPath);
      let scriptPath = path.join(path.dirname(notebookPath), cmd);
      let scriptDir = path.dirname(scriptPath);
      let moduleName = path.basename(scriptPath);
      let command = `if "${scriptDir}" not in sys.path:\n  sys.path.append("${scriptDir}")\n\n`;
      command += `try: reload(${moduleName})\nexcept: import ${moduleName}\n\n`;
      command += `from ${moduleName} import *\n`;

      return command;

      // command = `spec = importlib.util.spec_from_file_location('run.cell', '${scriptPath}')\n`;
      // command += "run_cell_module = importlib.util.module_from_spec(spec)\n";
      // command += "sys.modules['run.cell'] = run_cell_module\n";
      // command += "spec.loader.exec_module(run_cell_module)\n";
      // command += "from ${} import *\n";
    } else {
      vscode.window.showWarningMessage(
        "Cannot determine the noteboook source path"
      );
      return "";
    }
  }

  private prepareFsCommand(cellText: string): string {
    // For now only support a single line
    const subCmd = cellText.trim().split(/\n/g)[0].trim();
    const fsCmd = subCmd
      .replace(`\%${Magic.fs}`, "")
      .trim()
      .split(/\s/)
      .filter((v) => v.length > 0);
    const fsCmdStr = `${fsCmd[0]}("${fsCmd.slice(1).join('", "')}")`;
    return `w.dbutils.fs.${fsCmdStr}`;
  }

  private async createPythonRepl() {
    const config: vscode.WorkspaceConfiguration =
      vscode.workspace.getConfiguration("dbNotebook");
    const envCommand = config.get("pythonEnvActivationCommand", "");
    const showTerminal = config.get("showPythonRepl", false);

    const terminalOptions = {
      name: pythonReplName,
      hideFromUser: true,
    };

    let terminal = vscode.window.createTerminal(terminalOptions);
    terminal.show(showTerminal);

    if (envCommand && isString(envCommand)) {
      terminal.sendText(envCommand);
      await delay(config.get("envActivationTimeout", 2000));
    }

    terminal.sendText("python");
    await delay(config.get("pythonActivationCommand", 2000));

    terminal.sendText(pythonReplInitCommand);
    await delay(config.get("pythonActivationCommand", 2000));

    return terminal;
  }

  private runPythonCommand(
    command: string,
    doneFile: string,
    outMarker: string
  ): void {
    this.pythonRepl.sendText(`logger.clear_done("${doneFile}")`);
    this.pythonRepl.sendText(`logger.write("${outMarker}")`);
    this.pythonRepl.sendText(command);
    this.pythonRepl.sendText("\n");
    this.pythonRepl.sendText(`logger.flag_done("${doneFile}")`);
  }

  removePythonRepl() {
    this.pythonRepl = null;
  }

  private interrupt(_notebook: vscode.NotebookDocument): void {
    this.pythonRepl.sendText("\x03");
    this.interrupted = true;
  }
}

// This method is called when your extension is deactivated
export function deactivate() {}
