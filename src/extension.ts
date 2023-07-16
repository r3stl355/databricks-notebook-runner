import { TextDecoder, TextEncoder } from "util";
import * as vscode from "vscode";
import { readFile } from "fs/promises";
import { existsSync } from "fs";
import { randomBytes } from "crypto";
import { Terminal } from "vscode";
import * as path from "path";
import { tmpdir } from "os";

const notebookHeader = "# Databricks notebook source";
const cellSeparator = "\n# COMMAND ----------\n";
const cellSeparatorRegex = "\n# COMMAND -*[^\n]\n";

const magicHeader = "# MAGIC";
const mdMagicHeader = `${magicHeader} %md`;
const sqlMagicHeader = `${magicHeader} %sql`;
const runMagicHeader = `${magicHeader} %run`;
const shMagicHeader = `${magicHeader} %sh`;

const pythonLanguage = "python";
const sqlLanguage = "sql";
const runLanguage = "Run";
const shellLanguage = "Shell";
const mdLanguage = "markdown";

const randomName = () => randomBytes(16).toString("hex");
const isString = (obj: any) => typeof obj === "string";
const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));
const delaySync = (ms: number) =>
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

const logDir = `${tmpdir()}/db-notebook`;
const cmdOutput = getTmpFilePath(logDir, "cell-command", "out");

const pythonReplName = "Databricks Notebook (Python)";

const initReplCommand = `
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

// function getRandomName(): string {
//   return randomBytes(16).toString("hex");
// }

function getTmpFilePath(dir: string, namePrefix: string, ext: string) {
  if (!isString(dir)) {
    dir = tmpdir();
  }
  return path.join(dir, `${namePrefix}${randomName()}.${ext}`);
}

// let terminal: any = null;
let controller: any = null;

const normilizeCellText = (contents: string) =>
  normilizeCellTextArray(contents.split(/\n/g));

// .map((v) => v.replace(magicHeader, "").trim())
// .join("\n")
// .trim();

const normilizeCellTextArray = (contents: string[]) =>
  contents
    .map((v) => v.replace(magicHeader, "").trim())
    .join("\n")
    .trim();

interface RawNotebookCell {
  language: string;
  value: string;
  kind: vscode.NotebookCellKind;
}

export function activate(context: vscode.ExtensionContext) {
  // if (
  //   vscode.window.activeTextEditor === undefined ||
  //   !vscode.window.activeTextEditor.document
  //     .getText()
  //     .startsWith(notebookHeader)
  // ) {
  //   vscode.window.showErrorMessage("Open a Databricks notebook");
  //   return undefined;
  // }

  // Serializer
  context.subscriptions.push(
    vscode.workspace.registerNotebookSerializer(
      "databricks-notebook",
      new DatabricksNotebookSerializer()
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
    } catch {
      raw = [];
    }

    const cells = raw.map(
      (item) =>
        new vscode.NotebookCellData(item.kind, item.value, item.language)
    );

    return new vscode.NotebookData(cells);
  }

  async serializeNotebook(
    data: vscode.NotebookData,
    _token: vscode.CancellationToken
  ): Promise<Uint8Array> {
    let contents = data.cells
      .map((cell) => this.serializeCell(cell))
      .join(`\n\n${cellSeparator}\n\n`);

    return new TextEncoder().encode(notebookHeader + "\n" + contents);
  }

  private parseNotebook(contents: string): RawNotebookCell[] {
    if (contents.startsWith(notebookHeader)) {
      const headerPattern = new RegExp(notebookHeader, "g");
      const withoutHeader = contents.split(headerPattern)[1];
      // let splitted = withoutHeader.split(/\n# COMMAND -+[^\n]\n/g);
      const cellSeparatorPattern = new RegExp(cellSeparatorRegex, "g");
      const splitted = withoutHeader.split(cellSeparatorPattern);

      const cleaned = splitted
        .map((v) => v.replace(/^\s*[\r\n]/gm, ""))
        .filter((v) => v.length > 0)
        .map((v) => <RawNotebookCell>this.parseCell(v));
      return cleaned;
    } else {
      return [];
    }
  }

  private parseCell(contents: string): RawNotebookCell {
    // let kind = vscode.NotebookCellKind.Code;
    // let language = "python";
    // let v = contents;
    // let magicPattern = new RegExp(magicHeader, "gm");

    if (contents.startsWith(magicHeader)) {
      if (contents.startsWith(mdMagicHeader)) {
        return this.parseMdCell(contents);
      } else if (contents.startsWith(sqlMagicHeader)) {
        return this.parseSqlCell(contents);
      } else if (contents.startsWith(runMagicHeader)) {
        return this.parseRunCell(contents);
      } else if (contents.startsWith(shMagicHeader)) {
        return this.parseShCell(contents);
      } else {
        const magicPattern = new RegExp(magicHeader, "gm");
        const magic = contents
          .split(/\n/g)[0]
          .trim()
          .split(magicPattern)[1]
          .split(/\s/)[0];
        throw Error(`Magic ${magic} not supported`);
      }
    } else {
      return {
        kind: vscode.NotebookCellKind.Code,
        value: contents,
        language: pythonLanguage,
      };
    }

    // } else if (contents.startsWith(magicHeader)) {
    //   v = contents
    //     .split(/\n/g)
    //     .map((v) => v.replace(magicHeader, "").trim())
    //     .join("\n")
    //     .trim();
    // }
  }

  private parseMdCell(contents: string): RawNotebookCell {
    let value = normilizeCellTextArray(contents.split(/\n/g).slice(1));
    // .map((v) => v.replace(magicHeader, ""))
    // .join("\n")
    // .trim();
    return {
      kind: vscode.NotebookCellKind.Markup,
      value: normilizeCellTextArray(contents.split(/\n/g).slice(1)),
      language: mdLanguage,
    };
  }

  private parseSqlCell(contents: string): RawNotebookCell {
    // let value = contents
    //   .split(/\n/g)
    //   // .slice(1)
    //   .map((v) => v.replace(magicHeader, "").trim())
    //   .join("\n")
    //   .trim();
    return {
      kind: vscode.NotebookCellKind.Code,
      value: normilizeCellText(contents),
      language: sqlLanguage,
    };
  }

  private parseRunCell(contents: string): RawNotebookCell {
    // Must be a single-line but not enforcing here
    // let value = contents
    //   .split(/\n/g)
    //   .map((v) => v.replace(magicHeader, "").trim())
    //   .join("\n")
    //   .trim();
    return {
      kind: vscode.NotebookCellKind.Code,
      value: normilizeCellText(contents),
      language: runLanguage,
    };
  }

  private parseShCell(contents: string): RawNotebookCell {
    // let value = contents
    //   .split(/\n/g)
    //   .map((v) => v.replace(magicHeader, "").trim())
    //   .join("\n")
    //   .trim();
    return {
      kind: vscode.NotebookCellKind.Code,
      value: normilizeCellText(contents),
      language: shellLanguage,
    };
  }

  private serializeCell(cell: vscode.NotebookCellData): string {
    if (cell.kind === vscode.NotebookCellKind.Markup) {
      let split = cell.value.trim().split(/\n/gm);
      return `${mdMagicHeader}\n${magicHeader} ${split.join(
        `\n${magicHeader}`
      )}`;
    } else {
      if (cell.languageId === sqlLanguage) {
        let split = cell.value.trim().split(/\n/gm);
        return `${magicHeader} ${split.join(`\n${magicHeader}`)}`;
      } else if (cell.languageId === runLanguage) {
        // Technically, this shoud have only one line but not enforcing here
        let split = cell.value.trim().split(/\n/gm);
        return `${magicHeader} ${split.join(`\n${magicHeader}`)}`;
      } else if (cell.languageId === shellLanguage) {
        let split = cell.value
          .trim()
          .split(/\n/gm)
          .map((v) => `${magicHeader}${v.length > 0 ? ` ${v}` : ""}`);
        return split.join("\n");
      } else {
        return cell.value.trim();
      }
    }
  }
}

class Controller {
  readonly controllerId = "databricks-notebook-controller-id";
  readonly notebookType = "databricks-notebook";
  readonly label = "Databricks Notebook";
  readonly supportedLanguages = [
    pythonLanguage,
    sqlLanguage,
    shellLanguage,
    runLanguage,
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

  private async init() {
    this.pythonRepl = await this.createPythonRepl();
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
        if (language === runLanguage) {
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
    if (language === sqlLanguage) {
      command = command.replace("%sql", "").trim();
      if (command.length > 0) {
        command = `spark.sql("${command.replace(/\n/gm, " ")}").show()`;
      }
    } else if (language === runLanguage) {
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
    } else if (language === shellLanguage) {
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
      await delay(config.get("envActivationTimeout", 1000));
    }

    terminal.sendText("python");
    await delay(config.get("pythonActivationCommand", 1000));

    terminal.sendText(initReplCommand);

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

// function runCommand(command: string, doneFile: string, outMarker: string) {
//   terminal.sendText(`logger.clear_done("${doneFile}")`);
//   terminal.sendText(`logger.write("${outMarker}")`);
//   terminal.sendText(command);
//   terminal.sendText("\n");
//   terminal.sendText(`logger.flag_done("${doneFile}")`);
// }

// function removePythonTerminal() {
//   terminal = null;
//   // if (terminal !== null) {
//   //   terminal.sendText("exit()");
//   //   delaySync(300);
//   //   terminal.dispose();
//   //   terminal = null;
//   // }
// }

// This method is called when your extension is deactivated
export function deactivate() {}
