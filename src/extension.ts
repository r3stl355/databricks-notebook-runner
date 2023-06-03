import { TextDecoder, TextEncoder } from 'util';
import * as vscode from 'vscode';
import { readFile } from 'fs/promises';
import { existsSync } from 'fs';
import { randomBytes } from 'crypto';

const notebookHeader = "# Databricks notebook source";

const cellSeparator = "# COMMAND ----------";

const magicHeader = "# MAGIC ";
const mdMagicHeader = `${magicHeader}%md`;

const logDir = "/tmp/db-notebook";
const cmdOutput = `${logDir}/cell-command.out`;

const terminalSetup = `
export DATABRICKS_CONFIG_PROFILE=db-notebook
`;

const setLogger = `
import sys
import os

os.makedirs("${logDir}", exist_ok=True)

class my_logger:
  def __init__(self, out_file):
    self.out_file = out_file
    if os.path.exists(out_file):
      os.remove(out_file)
  def write(self, msg):
    with open(self.out_file, "a") as f:
      f.write(msg)
  def clear_done(self, done_file):
    if os.path.exists(done_file):
      os.remove(done_file)
  def flag_done(self, done_file):
    with open(done_file, "w") as f:
      f.write("DONE")

logger = my_logger("${cmdOutput}")

sys.stdout = logger
sys.stderr = logger


`;

let terminal: any = null;

const isString = (obj: any) => typeof obj === 'string';
const delay = (ms: number) => new Promise(res => setTimeout(res, ms));

export function activate(context: vscode.ExtensionContext) {
	context.subscriptions.push(
		vscode.workspace.registerNotebookSerializer('databricks-notebook', new SampleSerializer())
	);
  context.subscriptions.push(new Controller());
  createPythonTerminal();
  console.log("Databricks Notebook runner is now active!");
}

interface RawNotebookCell {
  language: string;
  value: string;
  kind: vscode.NotebookCellKind;
}

class SampleSerializer implements vscode.NotebookSerializer {

  async deserializeNotebook(content: Uint8Array, _token: vscode.CancellationToken): Promise<vscode.NotebookData> {
    var contents = new TextDecoder().decode(content);

    let raw: RawNotebookCell[];
    try {
      raw = <RawNotebookCell[]>parseNotebook(contents);
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
      .map(cell => serializeCell(cell))
      .join(`\n\n${cellSeparator}\n\n`);

    return new TextEncoder().encode(notebookHeader + "\n" + contents);
  }
}

function parseNotebook(contents: string): RawNotebookCell[] {
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
            v => <RawNotebookCell>(parseCell(v))
          );;
      return cleaned;
  } else {
    return [];
  }
}

function parseCell(contents: string): RawNotebookCell {
  let kind = vscode.NotebookCellKind.Code;
  let language = "python";
  let v = contents;
  if (contents.startsWith(mdMagicHeader)) {
    let magicPattern = new RegExp(magicHeader);
    v = contents
      .split(/\n/g)
      .slice(1)
      .map(
        v => v.replace(magicPattern, "")
      )
      .join("\n");

    // let mdPattern = new RegExp(mdMagicHeader, 'g');
    // let withoutMDHeader = contents.split(mdPattern)[1];
    // let magicPattern = new RegExp(magicHeader, 'gm');
    // v = withoutMDHeader.replace(magicPattern, "");
    kind = vscode.NotebookCellKind.Markup;
    language = "markdown";
  }
  return {"kind": kind, "value": v, "language": language};
}

function serializeCell(cell: vscode.NotebookCellData): string {
  if (cell.kind === vscode.NotebookCellKind.Markup) {
    let split = cell.value.split(/\n/gm);
    return `${mdMagicHeader}\n${magicHeader}${split.join(`\n${magicHeader}`)}`;
  }
  // TODO: handle non-Python code, e.g. `%sh` magic
  return cell.value;
}


class Controller {
  readonly controllerId = 'databricks-notebook-controller-id';
  readonly notebookType = 'databricks-notebook';
  readonly label = 'Databricks Notebook';
  readonly supportedLanguages = ['python'];

  private readonly _controller: vscode.NotebookController;
  private _executionOrder = 0;

  constructor() {
    this._controller = vscode.notebooks.createNotebookController(
      this.controllerId,
      this.notebookType,
      this.label
    );

    this._controller.supportedLanguages = this.supportedLanguages;
    this._controller.supportsExecutionOrder = true;
    this._controller.executeHandler = this._execute.bind(this);
  }

  dispose(): void {
		this._controller.dispose();
	}

  private _execute(cells: vscode.NotebookCell[], _notebook: vscode.NotebookDocument, _controller: vscode.NotebookController): void {
    for (let cell of cells) {
      this._doExecution(cell);
    }
  }

  private async _doExecution(cell: vscode.NotebookCell): Promise<void> {
    const execution = this._controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this._executionOrder;
    execution.start(Date.now());

    let command = execution.cell.document.getText();
    let cmdId = randomBytes(16).toString('hex');
    let flagFile = `${logDir}/flag-${cmdId}}`;
    runCommand(command, flagFile);

    const delayStep = 100;
    while (!existsSync(flagFile)) {
      await delay(delayStep);
    }
    let res = "OK";
    if (existsSync(cmdOutput)) {
      res = new TextDecoder().decode(await readFile(cmdOutput));
    }

    execution.replaceOutput([
      new vscode.NotebookCellOutput([
        vscode.NotebookCellOutputItem.text(res)
      ])
    ]);
    execution.end(true, Date.now());
  }
}

async function createPythonTerminal() {
  if (terminal === null) {
      const config: vscode.WorkspaceConfiguration = vscode.workspace.getConfiguration('databricksNotebook');
      const envCommand = config.get("environmentActivationCommand", "");

      const terminalOptions = {
          name: "Databricks Notebook",
          hideFromUser: true
      };

      terminal = vscode.window.createTerminal(terminalOptions);
      terminal.show(true);

      if (terminalSetup && isString(terminalSetup)){
        terminal.sendText(terminalSetup);
      }
      await delay(config.get("terminalInitTimeout", 1000));

      if (envCommand && isString(envCommand)){
        terminal.sendText(envCommand);
      }
      await delay(config.get("terminalInitTimeout", 1000));

      terminal.sendText("python");
      await delay(config.get("pythonCommandTimeout", 300));

      terminal.sendText(setLogger);
  }
}

function removePythonTerminal() {
  terminal = null;
}

function runCommand(command: string, doneFile: string) {
  terminal.sendText(`logger.clear_done("${doneFile}")`);
  terminal.sendText(command);
  terminal.sendText("\n"); 
  terminal.sendText(`logger.flag_done("${doneFile}")`); 
}


// This method is called when your extension is deactivated
export function deactivate() {
  removePythonTerminal();
}
