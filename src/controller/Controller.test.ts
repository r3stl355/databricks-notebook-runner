import * as assert from "assert";
import { mock, when, instance, anything } from "ts-mockito";
import {
  Disposable,
  ExtensionContext,
  NotebookController,
  Terminal,
} from "vscode";
import { Controller } from "./Controller";
import { Language } from "../common/Types";

describe(__filename, () => {
  let disposables: Array<Disposable>;
  let controller: Controller;
  let notebookControllerMock: NotebookController;

  beforeEach(async () => {
    disposables = [];

    notebookControllerMock = mock<NotebookController>();
    const notebookController = instance(notebookControllerMock);
    controller = new Controller(notebookController);
  });

  afterEach(() => {
    disposables.forEach((d) => d.dispose());
  });

  it("should parse simple SQL correctly", async () => {
    const sql = "SELECT * FROM test";
    // @ts-ignore
    let cmd = controller.prepareCommand(`%sql\n${sql}`, Language.sql);
    assert.strictEqual(cmd, `spark.sql("${sql}").show()`);
  });

  it("should send a command to terminal", async () => {
    let replText: string = "";
    let replMoc = mock<Terminal>();
    when(replMoc.sendText(anything())).thenCall(
      (cmd) => (replText += `${cmd}`)
    );

    // @ts-ignore
    controller.pythonRepl = instance(replMoc);
    // @ts-ignore
    controller.runPythonCommand("test-command");

    assert(replText.indexOf("test-command") > 0 === true);
  });
});
