import { TextDecoder, TextEncoder } from "util";
import * as vscode from "vscode";
import { readFile } from "fs/promises";
import { existsSync } from "fs";
import { randomBytes } from "crypto";
import { Terminal } from "vscode";
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
}
// const pythonLanguage = "python";
// const sqlLanguage = "sql";
// const runLanguage = "run";
// const shLanguage = "shellscript";
// const remoteShLanguage = "remote-sh";
// // const mdLanguage = "markdown";
// // const undefinedLanguage = "plaintext";

// const undefinedKind = vscode.NotebookCellKind.Code;

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
}

interface RawNotebookCell {
  magic: Magic;
  language: Language;
  value: string;
  kind: Kind;
}

const magicHeader = "# MAGIC";
const mdMagicHeader = `${magicHeader} %${Magic.md}`;

// const magicMap = new Map([
//   [Magic.md, { language: Language.md, kind: vscode.NotebookCellKind.Markup }],
//   [Magic.sql, { language: Language.sql, kind: vscode.NotebookCellKind.Code }],
//   [Magic.run, { language: Language.run, kind: vscode.NotebookCellKind.Code }],
//   [Magic.sh, { language: Language.sh, kind: vscode.NotebookCellKind.Code }],
//   [
//     Magic.none,
//     { language: Language.python, kind: vscode.NotebookCellKind.Code },
//   ],
//   [
//     Magic.invalid,
//     { language: Language.unknown, kind: vscode.NotebookCellKind.Code },
//   ],
// ]);

const magicMap = new Map([
  [Magic.md, { language: Language.md, kind: Kind.markdown }],
  [Magic.sql, { language: Language.sql, kind: Kind.code }],
  [Magic.run, { language: Language.run, kind: Kind.code }],
  [Magic.sh, { language: Language.sh, kind: Kind.code }],
  [Magic.none, { language: Language.python, kind: Kind.code }],
  [Magic.invalid, { language: Language.unknown, kind: Kind.code }],
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
  return contents.replace(magicPattern, "");
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
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language 
#from databricks.sdk import dbutils

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

// const normilizeCellText = (contents: string) =>
//   normilizeCellTextArray(contents.split(/\n/g));

// const normilizeCellTextArray = (contents: string[]) =>
//   contents
//     .map((v) => v.replace(magicHeader, "").trim())
//     .join("\n")
//     .trim();
// const removeMagicHeaders = (contents: string[]) =>
//   contents
//     .map((v) => v.replace(magicHeader, "").trim())
//     .join("\n")
//     .trim();

// contents.replace()
//   .split(/\n/g)
//   .map((v) => v.replace(`${magicHeader} `, ""))
//   .join("\n")
//   .trim();

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
  controller.init();
  context.subscriptions.push(controller);
  vscode.window.showInformationMessage("Databricks Notebook runner is active!");

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

  // private parseCell(contents: string): RawNotebookCell {
  //   if (contents.trimStart().startsWith(magicHeader)) {
  //     return this.parseMagicCell(contents);
  //   }
  //   return {
  //     magic: "",
  //     kind: vscode.NotebookCellKind.Code,
  //     value: contents,
  //     language: pythonLanguage,
  //   };
  // }

  // private parseMagicCell(contents: string): RawNotebookCell {
  //   const leftTrimmed = contents.trimStart().split(/\n/g);
  //   let parsedMagic = parseMagic(leftTrimmed[0]);
  //   if (parsedMagic.language === undefinedLanguage) {
  //     vscode.window.showErrorMessage(
  //       `Magic ${parsedMagic.magic} not supported`
  //     );
  //   }
  //   if (parsedMagic.kind === vscode.NotebookCellKind.Markup) {
  //     // Remove `%md` from the markdown cell, anything before the `%md` row is irrelevant
  //     contents = leftTrimmed.slice(1).join("\n");
  //   }
  //   return {
  //     magic: parsedMagic.magic,
  //     kind: parsedMagic.kind,
  //     // value: normilizeCellTextArray(split), // TODO: don't need this here, move to execute
  //     value: removeMagicHeaders(contents),
  //     language: parsedMagic.language,
  //   };
  // }

  private serializeCell(cell: vscode.NotebookCellData): string {
    // Important design decition - current cell content takes over the cell properties.
    // This way the cell type can be changed and enforced via magic strings
    const rawCell = parseCellText(cell.value, false);
    if (rawCell.magic === Magic.none && rawCell.language === Language.python) {
      return cell.value;
    }
    const split = cell.value.split(/\n/gm);
    let res = `${magicHeader} ${split.join(`\n${magicHeader} `)}`;

    if (
      rawCell.kind !== Kind.markdown &&
      cell.kind === vscode.NotebookCellKind.Markup
    ) {
      res = `${mdMagicHeader}\n${res}`;
    }
    return res;

    // else {
    // const language =
    //   (rawCell.language === cell.languageId || rawCell.magic === Magic.sh)
    //     ? cell.languageId
    //     : rawCell.language;
    //   const;
    //   if (language === Language.sql) {
    //     let split = cell.value.trim().split(/\n/gm);
    //     return `${magicHeader} ${split.join(`\n${magicHeader} `)}`;
    //   } else if (language === Language.run) {
    //     // Technically, this shoud have only one line but not enforcing here
    //     let split = cell.value.trim().split(/\n/gm);
    //     return `${magicHeader} ${split.join(`\n${magicHeader} `)}`;
    //   } else if (cell.languageId === Language.sh) {
    //     let split = cell.value
    //       .trim()
    //       .split(/\n/gm)
    //       .map((v) => `${magicHeader}${v.length > 0 ? ` ${v}` : ""}`);
    //     return split.join("\n");
    //   } else {
    //     return cell.value;
    //   }
    // }
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
  ];

  private readonly controller: vscode.NotebookController;
  private pythonRepl: any;
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
    this.createPythonRepl().then((repl) => (this.pythonRepl = repl));
  }

  private async execute(
    cells: vscode.NotebookCell[],
    _notebook: vscode.NotebookDocument,
    _controller: vscode.NotebookController
  ): Promise<void> {
    this.interrupted = false;
    for (let cell of cells) {
      await this.executeCell(cell);
    }
  }

  private async executeCell(cell: vscode.NotebookCell): Promise<void> {
    let cellText = cell.document.getText().trim();

    if (cellText.length === 0) {
      return;
    }
    let language = cell.document.languageId;

    // let language = execution.cell.document.languageId;
    let command = await this.prepareCommand(cellText, language);
    if (command.length === 0) {
      return;
    }

    const execution = this.controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this._executionOrder;
    execution.start(Date.now());

    let cmdId = randomName(); // randomBytes(16).toString("hex");
    let flagFile = `${logDir}/flag-${cmdId}`;
    let markerStart = "-- db-notebook-";
    let marker = `${markerStart}${cmdId} -->`;

    this.runCommand(command, flagFile, marker);

    const delayStep = 100;
    while (!existsSync(flagFile) && !this.interrupted) {
      await delay(delayStep);
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

  private async prepareCommand(
    cellText: string,
    language: string
  ): Promise<string> {
    let command = cellText;
    if (language === Language.sql) {
      command = command.replace("%sql", "").trim();
      if (command.length > 0) {
        command = `spark.sql("${command.replace(/\n/gm, " ")}").show()`;
      }
    } else if (language === Language.run) {
      let notebookPath = vscode.window.activeTextEditor?.document.uri.path!;
      if (isString(notebookPath)) {
        let cmd = command.replace("%run ", "").trim();
        let dir = path.dirname(notebookPath);
        let scriptPath = path.join(path.dirname(notebookPath), cmd);
        let scriptDir = path.dirname(scriptPath);
        let moduleName = path.basename(scriptPath);
        command = `if "${scriptDir}" not in sys.path:\n  sys.path.append("${scriptDir}")\n\n`;
        command += `try: reload(${moduleName})\nexcept: import ${moduleName}\n\n`;
        command += `from ${moduleName} import *\n`;

        // command = `spec = importlib.util.spec_from_file_location('run.cell', '${scriptPath}')\n`;
        // command += "run_cell_module = importlib.util.module_from_spec(spec)\n";
        // command += "sys.modules['run.cell'] = run_cell_module\n";
        // command += "spec.loader.exec_module(run_cell_module)\n";
        // command += "from ${} import *\n";
      } else {
        vscode.window.showWarningMessage(
          "Cannot determine the noteboook source path"
        );
      }
    } else if (language === Language.remoteSh) {
      let cmd = command.replace("%sh", "").trim().split(/\n/gm);
      command = "w.clusters.ensure_cluster_is_running(w.config.cluster_id)\n";
      command += `c = w.command_execution\n`;
      command += `c_id = c.create_and_wait(cluster_id=w.config.cluster_id, language=lang).id\n`;
      let subCmd = cmd.map(
        (v) =>
          `import subprocess\\nsubprocess.run(['${v
            .trim()
            .replace(" ", "','")}'], capture_output=True).stdout.decode()`
      );
      command += subCmd
        .map(
          (v) =>
            `print(c.execute_and_wait(context_id=c_id, cluster_id=w.config.cluster_id, language=lang, command="${v}").results.data)`
        )
        .join("\n");
    }
    return command;
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

    return terminal;
  }

  private runCommand(
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
