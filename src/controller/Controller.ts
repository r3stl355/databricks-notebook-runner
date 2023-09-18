import { tmpdir } from "os";
import { TextDecoder, TextEncoder } from "util";
import { basename, dirname, join } from "path";
import { readFile } from "fs/promises";
import { existsSync } from "fs";
import {
  NotebookCell,
  NotebookDocument,
  NotebookController,
  NotebookCellOutput,
  NotebookCellOutputItem,
  window,
  workspace,
  WorkspaceConfiguration,
} from "vscode";
import { Language, Magic } from "../common/Types";
import * as u from "../common/Utils";

export const pythonReplName = "Databricks Notebook Runner (Python)";

const pythonReplInitCommand = (logDir: string, commandOutputFile: string) => `
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

logger = my_logger("${commandOutputFile}")

spark = None

try:
  from databricks.connect import DatabricksSession
  spark = DatabricksSession.builder.getOrCreate()
except e:
  print(f"Unable to create a remote Spark Session: {e}")

sys.stdout = logger
sys.stderr = logger

w = WorkspaceClient()
lang = Language.PYTHON
`;

export class Controller {
  readonly supportedLanguages = [
    Language.python,
    Language.sql,
    Language.sh,
    Language.remoteSh,
    Language.run,
    Language.fs,
  ];

  private readonly controller: NotebookController;
  private logDir: string;
  private commandOutputFile: string;
  private pythonRepl: any;
  private initialised: boolean = false;
  private executionOrder = 0;
  private interrupted = false;

  constructor(notebookController: NotebookController) {
    this.controller = notebookController;
    this.logDir = join(tmpdir(), "db-notebook");
    this.commandOutputFile = u.getTmpFilePath(
      this.logDir,
      "cell-command",
      "out"
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
    this.pythonRepl = await this.createPythonRepl(pythonReplName);
    this.initialised = true;
  }

  private async execute(
    cells: NotebookCell[],
    _notebook: NotebookDocument,
    _controller: NotebookController
  ): Promise<void> {
    this.interrupted = false;
    while (!this.initialised && !this.interrupted) {
      await u.delay(100);
    }
    if (this.interrupted) {
      return;
    }
    if (this.pythonRepl === null) {
      window.showErrorMessage("Python REPL was closed, reload the extension");
      return;
    }
    for (let cell of cells) {
      await this.executeCell(cell);
    }
  }

  private async executeCell(cell: NotebookCell): Promise<void> {
    let cellText = cell.document.getText().trim();

    if (cellText.length === 0) {
      return;
    }
    // Important design decision - current cell content takes over the cell properties.
    // This way the cell type can be changed and enforced via magic strings
    const rawCell = u.parseCellText(cellText, false);

    // Some magic is used by more than one language (e.g. `sh`)
    let language = rawCell.language;
    if (
      rawCell.magic === Magic.sh &&
      cell.document.languageId !== rawCell.language &&
      cell.document.languageId === Language.remoteSh
    ) {
      language = Language.remoteSh;
    }
    const command = this.prepareCommand(cellText, language, rawCell.magic);
    if (command.length > 0) {
      await this.executeCommand(
        cell,
        u.enumFromString(Language, language)!,
        command
      );
    }
  }

  private async executeCommand(
    cell: NotebookCell,
    language: Language,
    command: string
  ): Promise<void> {
    const execution = this.controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this.executionOrder;
    execution.start(Date.now());

    let cmdId = u.randomName();
    let flagFile = join(this.logDir, `flag-${cmdId}`);
    let markerStart = "-- db-notebook-";
    let marker = `${markerStart}${cmdId} -->`;

    this.runPythonCommand(command, flagFile, marker);

    const statusCheckDelay = 100;
    while (!existsSync(flagFile) && !this.interrupted) {
      await u.delay(statusCheckDelay);
    }

    let res = "OK";
    if (this.interrupted) {
      res = "Cancelled";
    } else if (existsSync(this.commandOutputFile)) {
      let output = new TextDecoder().decode(
        await readFile(this.commandOutputFile)
      );
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
      new NotebookCellOutput([NotebookCellOutputItem.text(res)]),
    ]);
    execution.end(true, Date.now());
  }

  private prepareCommand(
    cellText: string,
    language: Language,
    magic: Magic
  ): string {
    if (language === Language.sql) {
      return this.prepareSqlCommand(cellText);
    } else if (language === Language.run) {
      return this.prepareRunCommand(cellText);
    } else if (language === Language.sh) {
      return magic === Magic.pip
        ? this.preparePipCommand(cellText)
        : this.prepareShCommand(cellText);
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
    return this.splitStripEmpty(cellText.replace(`%sh`, ""))
      .map(
        (v) =>
          `subprocess.run(['${v
            .split(" ")
            .filter((v1) => v1.length > 0)
            .join("', '")}'], capture_output=True).stdout.decode()`
      )
      .join("\n");
  }

  private preparePipCommand(cellText: string): string {
    // Expect only a single line
    const cmd = this.splitStripEmpty(cellText.replace("%pip", ""))[0].trim();
    if (cmd.length === 0) {
      return "";
    }
    return `subprocess.run(['pip', '${cmd
      .split(" ")
      .filter((v) => v.length > 0)
      .join("', '")}'], capture_output=True).stdout.decode()`;
  }

  private prepareRemoteShCommand(cellText: string): string {
    let command = "w.clusters.ensure_cluster_is_running(w.config.cluster_id)\n";
    command += `c = w.command_execution\n`;
    command += `c_id = c.create_and_wait(cluster_id=w.config.cluster_id, language=lang).id\n`;
    let subCmd = this.splitStripEmpty(cellText.replace("%sh", ""))
      .map(
        (v) =>
          `import subprocess\\nsubprocess.run(['${v
            .split(" ")
            .filter((v1) => v1.length > 0)
            .join("', '")}'], capture_output=True).stdout.decode()`
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
    let notebookPath = window.activeTextEditor?.document.uri.path!;
    if (u.isString(notebookPath)) {
      let cmd = cellText.replace("%run ", "").trim();
      let dir = dirname(notebookPath);
      let scriptPath = join(dirname(notebookPath), cmd);
      let scriptDir = dirname(scriptPath);
      let moduleName = basename(scriptPath);
      let command = `if "${scriptDir}" not in sys.path:\n  sys.path.append("${scriptDir}")\n\n`;
      command += `try: reload(${moduleName})\nexcept: import ${moduleName}\n\n`;
      command += `from ${moduleName} import *\n`;

      return command;
    } else {
      window.showWarningMessage("Cannot determine the noteboook source path");
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

  private async createPythonRepl(replName: string) {
    const config: WorkspaceConfiguration =
      workspace.getConfiguration("dbNotebook");
    const envCommand = config.get("pythonEnvActivationCommand", "");
    const showTerminal = config.get("showPythonRepl", false);

    const terminalOptions = {
      name: replName,
      hideFromUser: showTerminal,
    };

    let terminal = window.createTerminal(terminalOptions);
    if (showTerminal) {
      terminal.show(true);
    }

    if (envCommand && u.isString(envCommand)) {
      terminal.sendText(envCommand);
      await u.delay(config.get("envActivationTimeout", 2000));
    }

    terminal.sendText("python");
    await u.delay(config.get("pythonActivationCommand", 2000));

    terminal.sendText(
      pythonReplInitCommand(this.logDir, this.commandOutputFile)
    );
    await u.delay(config.get("pythonActivationCommand", 2000));

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

  private interrupt(_notebook: NotebookDocument): void {
    this.pythonRepl.sendText("\x03");
    this.interrupted = true;
  }
}
