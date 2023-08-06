import { notebooks, workspace, ExtensionContext, window } from "vscode";
import { DatabricksNotebookSerializer } from "./serializer/DatabricksNotebookSerializer";
import { Controller, pythonReplName } from "./controller/Controller";
import * as c from "./common/Constants";

export function activate(context: ExtensionContext) {
  // Serializer
  const serializer = new DatabricksNotebookSerializer();
  context.subscriptions.push(
    workspace.registerNotebookSerializer("db-notebook", serializer)
  );

  // Controller
  let notebookController = notebooks.createNotebookController(
    c.controllerId,
    c.notebookType,
    c.controllerLabel
  );
  let controller = new Controller(notebookController);
  context.subscriptions.push(controller);
  controller
    .init()
    .then(() =>
      window.showInformationMessage("Databricks Notebook Runner is active!")
    );

  window.onDidCloseTerminal(function (event) {
    if (event.name === pythonReplName && controller !== null) {
      controller.removePythonRepl();
    }
  });
}

// This method is called when your extension is deactivated
export function deactivate() {}
