import { TextDecoder, TextEncoder } from 'util';
import * as vscode from 'vscode';
import { readFile } from 'fs/promises';
import { existsSync } from 'fs';
import { randomBytes } from 'crypto';
import { Terminal } from 'vscode';
import * as path from 'path';

const notebookHeader = "# Databricks notebook source";

const cellSeparator = "# COMMAND ----------";

const magicHeader = "# MAGIC";
const mdMagicHeader = `${magicHeader} %md`;
const sqlMagicHeader = `${magicHeader} %sql`;
const runMagicHeader = `${magicHeader} %run`;
const shMagicHeader = `${magicHeader} %sh`;
const runLanguage = "RUN";

const logDir = "/tmp/db-notebook";
const cmdOutput = `${logDir}/cell-command.out`;

const terminalName = 'Databricks Notebook';

const setLogger = `
from importlib import reload
import os, sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language 

sys.path.append("${logDir}")

os.makedirs("${logDir}", exist_ok=True)

class my_logger:
  def __init__(self, out_file):
    self.out_file = out_file
    #if os.path.exists(out_file):
    #  os.remove(out_file)
  def write(self, msg):
    with open(self.out_file, "a") as f:
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

let terminal: any = null;

const isString = (obj: any) => typeof obj === 'string';
const delay = (ms: number) => new Promise(res => setTimeout(res, ms));
const delaySync = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

interface RawNotebookCell {
  language: string;
  value: string;
  kind: vscode.NotebookCellKind;
}

export function activate(context: vscode.ExtensionContext) {
  vscode.window.onDidCloseTerminal(function (event) {
      if (event.name === terminalName) {
          removePythonTerminal();
      }
  });

	context.subscriptions.push(
		vscode.workspace.registerNotebookSerializer('databricks-notebook', new DatabricksNotebookSerializer())
	);
  context.subscriptions.push(new Controller());
  createPythonTerminal();
  vscode.window.showInformationMessage("Databricks Notebook runner is active!");
}

class DatabricksNotebookSerializer implements vscode.NotebookSerializer {

  async deserializeNotebook(content: Uint8Array, _token: vscode.CancellationToken): Promise<vscode.NotebookData> {
    var contents = new TextDecoder().decode(content);

    let raw: RawNotebookCell[];
    try {
      raw = <RawNotebookCell[]>this.parseNotebook(contents);
    } catch {
      raw = [];
    }

    const cells = raw.map(
      item => new vscode.NotebookCellData(item.kind, item.value, item.language)
    );

    return new vscode.NotebookData(cells);
  }

  async serializeNotebook(data: vscode.NotebookData, _token: vscode.CancellationToken): Promise<Uint8Array> {
    let contents = data.cells
      .map(cell => this.serializeCell(cell))
      .join(`\n\n${cellSeparator}\n\n`);

    return new TextEncoder().encode(notebookHeader + "\n" + contents);
  }
  
  private parseNotebook(contents: string): RawNotebookCell[] {
    if (contents.startsWith(notebookHeader)) {
        let headerPattern = new RegExp(notebookHeader, 'g');
        let withoutHeader = contents.split(headerPattern)[1];
        let splitted = withoutHeader.split(/# COMMAND -+/g);
        let cleaned = splitted
            .map(
                v => v.replace(/^\s*[\r\n]/gm, "")  
            ).filter(
                v => v.length > 0
            ).map(
              v => <RawNotebookCell>(this.parseCell(v))
            );;
        return cleaned;
    } else {
      return [];
    }
  }

  private parseCell(contents: string): RawNotebookCell {
    let kind = vscode.NotebookCellKind.Code;
    let language = "python";
    let v = contents;
    let magicPattern = new RegExp(magicHeader, "gm");

    if (contents.startsWith(mdMagicHeader)) {
      v = contents
        .split(/\n/g)
        .slice(1)
        .map(
          v => v.replace(magicHeader, "")
        )
        .join("\n").trim();
      kind = vscode.NotebookCellKind.Markup;
      language = "markdown";
    } else if (contents.startsWith(sqlMagicHeader)) {
      v = contents
        .split(/\n/g)
        // .slice(1)
        .map(
          v => v.replace(magicHeader, "").trim()
        )
        .join("\n").trim();

      kind = vscode.NotebookCellKind.Code;
      language = "SQL";
    } else if (contents.startsWith(runMagicHeader)) {
      // Must be a single-line but not enforcing here
      v = contents
        .split(/\n/g)
        .map(
          v => v.replace(magicHeader, "").trim()
        )
        .join("\n").trim();

      kind = vscode.NotebookCellKind.Code;
      language = runLanguage;
    } else if (contents.startsWith(shMagicHeader)) {
      v = contents
        .split(/\n/g)
        .map(
          v => v.replace(magicHeader, "").trim()
        )
        .join("\n").trim();

      kind = vscode.NotebookCellKind.Code;
      language = "SHELL";
    } else if (contents.startsWith(magicHeader)) {
      v = contents
        .split(/\n/g)
        .map(
          v => v.replace(magicHeader, "").trim()
        )
        .join("\n").trim();
    }

    return {"kind": kind, "value": v, "language": language};
  }

  private serializeCell(cell: vscode.NotebookCellData): string {
    if (cell.kind === vscode.NotebookCellKind.Markup) {
      let split = cell.value.trim().split(/\n/gm);
      return `${mdMagicHeader}\n${magicHeader} ${split.join(`\n${magicHeader}`)}`;
    }
    else {
      if (cell.languageId === "SQL") {
        let split = cell.value.trim().split(/\n/gm);
        return `${magicHeader} ${split.join(`\n${magicHeader}`)}`;
      } else if (cell.languageId === runLanguage) {
        // Technically, this shoud have only one line but not enforcing here
        let split = cell.value.trim().split(/\n/gm);
        return `${magicHeader} ${split.join(`\n${magicHeader}`)}`;
      } else if (cell.languageId === "SHELL") {
        let split = cell.value.trim()
                      .split(/\n/gm)
                      .map(
                        v => `${magicHeader}${v.length > 0 ?  ` ${v}`: ""}`
                      );
        return split.join("\n");
      }
      else {
        return cell.value.trim();
      }
    }
  }
}

class Controller {
  readonly controllerId = 'databricks-notebook-controller-id';
  readonly notebookType = 'databricks-notebook';
  readonly label = 'Databricks Notebook';
  readonly supportedLanguages = ["python", "SQL", "SHELL", runLanguage];

  private readonly _controller: vscode.NotebookController;
  private _executionOrder = 0;
  private interrupted = false;

  constructor() {
    this._controller = vscode.notebooks.createNotebookController(
      this.controllerId,
      this.notebookType,
      this.label
    );

    this._controller.supportedLanguages = this.supportedLanguages;
    this._controller.supportsExecutionOrder = true;
    this._controller.executeHandler = this._execute.bind(this);
    this._controller.interruptHandler = this._interrupt.bind(this);
  }

  dispose(): void {
		this._controller.dispose();
	}

  private _execute(cells: vscode.NotebookCell[], _notebook: vscode.NotebookDocument, _controller: vscode.NotebookController): void {
    this.interrupted = false;
    for (let cell of cells) {
      this._doExecution(cell);
    }
  }

  private async _doExecution(cell: vscode.NotebookCell): Promise<void> {

    let command = cell.document.getText().trim();
    if (command.length === 0) { // || command.replace("%sql", "").trim().length === 0) {
      return;
    }

    const execution = this._controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this._executionOrder;
    execution.start(Date.now());

    let language = execution.cell.document.languageId;
    let cmdId = randomBytes(16).toString('hex');
    let flagFile = `${logDir}/flag-${cmdId}}`;
    let markerStart = "-- db-notebook-";
    let marker = `${markerStart}${cmdId} -->}`;

    if (language === "SQL") {
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
        vscode.window.showWarningMessage("Cannot determine the noteboook source path");
      }
    } else if (language === "SHELL") {
      let cmd = command.replace("%sh", "").trim().split(/\n/mg);
      command = "w.clusters.ensure_cluster_is_running(w.config.cluster_id)\n";
      command += `c = w.command_execution\n`;
      command += `c_id = c.create_and_wait(cluster_id=w.config.cluster_id, language=lang).id\n`;
      let subCmd = cmd.map(
          v => `import subprocess\\nsubprocess.run(['${v.trim().replace(" ", "','")}'], capture_output=True).stdout.decode()`
      );
      command += subCmd.map(
          v => `print(c.execute_and_wait(context_id=c_id, cluster_id=w.config.cluster_id, language=lang, command="${v}").results.data)`
        ).join("\n");  
        
    }
    runCommand(command, flagFile, marker);

    const delayStep = 100;
    while (!existsSync(flagFile) && !this.interrupted) {
      await delay(delayStep);
    }

    let res = "OK";
    if (this.interrupted) {
      res = "Cancelled";
    }
    else if (existsSync(cmdOutput)) {
      let output = new TextDecoder().decode(await readFile(cmdOutput));
      if (output.match(marker)) {
        res = output.split(marker)[1].split(markerStart)[0];
        if (language === runLanguage) {
          // Filter out the response from reload
          res = res
            .split(/\n/gm)
            .filter(v => !(v.startsWith("<module ") && v.endsWith(">")))
            .join("\n");
        }
      }
      if (res.length === 0) {
        res = "OK";
      }
    }

    execution.replaceOutput([
      new vscode.NotebookCellOutput([
        vscode.NotebookCellOutputItem.text(res)
      ])
    ]);
    execution.end(true, Date.now());
  }

  private _interrupt(_notebook: vscode.NotebookDocument): void {
    terminal.sendText('\x03');
    this.interrupted = true;
  }
}
  
async function createPythonTerminal() {
  if (terminal === null) {
      const config: vscode.WorkspaceConfiguration = vscode.workspace.getConfiguration('dbNotebook');
      const envCommand = config.get("environmentActivationCommand", "");

      const terminalOptions = {
          name: terminalName,
          hideFromUser: true
      };

      terminal = vscode.window.createTerminal(terminalOptions);
      terminal.show(true);

      if (envCommand && isString(envCommand)){
        terminal.sendText(envCommand);
      }
      await delay(config.get("terminalInitTimeout", 300));

      terminal.sendText("python");
      await delay(config.get("pythonCommandTimeout", 300));

      terminal.sendText(setLogger);
  }
}

function runCommand(command: string, doneFile: string, outMarker: string) {
  terminal.sendText(`logger.clear_done("${doneFile}")`);
  terminal.sendText(`logger.write("${outMarker}")`);
  terminal.sendText(command);
  terminal.sendText("\n"); 
  terminal.sendText(`logger.flag_done("${doneFile}")`); 
}

function removePythonTerminal() {
  terminal = null;
  // if (terminal !== null) {
  //   terminal.sendText("exit()");
  //   delaySync(300);
  //   terminal.dispose();
  //   terminal = null;
  // }
}

// This method is called when your extension is deactivated
export function deactivate() {}
