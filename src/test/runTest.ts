import { join, resolve } from "path";
import { mkdir } from "fs/promises";
import { tmpdir } from "os";

import { downloadAndUnzipVSCode, runTests } from "@vscode/test-electron";

async function main() {
  try {
    // The folder containing the Extension Manifest package.json
    // Passed to `--extensionDevelopmentPath`
    const extensionDevelopmentPath = resolve(__dirname, "../../");

    const cachePath = join(tmpdir(), "db-notebook-tests");
    await mkdir(cachePath, { recursive: true });

    const vscodeExecutablePath = await downloadAndUnzipVSCode({
      version: process.env.VSCODE_TEST_VERSION || "stable",
      cachePath,
    });

    // The path to test runner
    // Passed to --extensionTestsPath
    const extensionTestsPath = resolve(__dirname, "./suite");

    // Download VS Code, unzip it and run the integration test
    await runTests({ extensionDevelopmentPath, extensionTestsPath });
  } catch (err) {
    console.error("Failed to run tests", err);
    process.exit(1);
  }
}

main();
