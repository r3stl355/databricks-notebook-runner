# Databricks Notebook Runner Extension for Visual Studio Code

The Databricks Notebook Runner Extension for Visual Studio Code allows you to run Databricks notebooks as interactive notebooks in Visual Studio Code.

> 📘 **Notes**:
>
> - This project is Experimental.
> - This project is **not** developed or supported by Databricks. The word `Databricks` in the name is used to merely classify the type of notebooks it can run.
> - A word **Extension** is used in this document to abbreviate, and have the same meaning as **Databricks Notebook Runner Extension**.

## Features

- Run Databricks notebooks on the local development machine in Visual Studio Code without any modifications
- Support for hybrid execution - on the local machine or a remote Databricks cluster
- The following Databricks-specific variables are instantiated through Databricks Connect integration and available by default. They are expected to work in the same way they do when a Databricks notebook is executed on the Databricks Platform:

- `spark`, an instance of `databricks.connect.DatabricksSession`
- `dbutils`, an instance of Databricks Utilities

- The following notebook magics are supported:
- `%frun`: runs a target Python file in the context of the current notebook. It can be any Python file, not necessarily a Databricks notebook only
- `%fs`: runs `dbutils.fs`
- `%sh`: runs a given Shell command on the local machine, or in the remote Databricks workspace if the `Remote Shell` cell language is selected
- `%md`: parses and displays the cell content as Markdown
- `%sql`: runs the cell content as an SQL statement using `spark.sql`
- `%pip`: runs `pip` command on the local machine

## Limitations

- The `%run` and `%fs` magics are expected to be single-line with only one of them per cell
- Any magic which is not listed in the Features section (e.g. `%scala` magic) is not supported and will be treated as Text
- Databricks notebooks are Python files with `.py` file extension (with some special comments to provide additional structure), which means that the Extension will attempt to parse any Python file. If the file is not a valid Databricks Notebook then it will be parsed as a notebook with a single cell. To use a default VSCode parser for those files you may need to disable the Databricks Notebook Runner Extension.

## Requirements

- A Databricks Workspace with
  - a pre-configured Databricks cluster running Databricks Runtime 13.0 or higher
  - a Databricks personal access token (PAT) for a user who has access to the Databricks cluster
- Visual Studio Code 1.80.2 or higher (the Extension may still work with the earlier versions but that was tested)
- Visual Studio Code configured to work with Python
- A Python environment (Python virtual environment is recommended but not enforced) with:
- Databricks Connect installed and configured. For more information see [Databricks Connect reference](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html)

## Quick start

- Install and configure Databricks Connect as described in [Databricks Connect reference](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html). There are a few ways to configure Databricks Connect, see [An example of configuring Databricks Connect](#an-example-of-configuring-databricks-connect) section down below for a simple example.
- In Visual Studio Code, search for `Databricks Notebook Runner` via the `Search Extensions in Marketplace` (accessible through the `Extensions` button in the main `Activity` panel) and install the extension.
- Open the folder containing Databricks notebooks. A pair of sample notebooks are included in this repository under `notebooks`
- Configure the Extension. Add the following settings<sup>\*</sup> to Visual Studio Code (or just configure your environment to match the default values). For more information see [settings.json](https://code.visualstudio.com/docs/getstarted/settings#_settingsjson):

| Setting                                 | Description                                                                           | Default Value                                                                                                                                                                                                                                                                 |
| :-------------------------------------- | :------------------------------------------------------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dbNotebook.pythonEnvActivationCommand` | Custom Python environment activation command (must have Databricks Connect installed) | `export DATABRICKS_CONFIG_PROFILE=db-notebook && workon dbconnect_env`</code><pre><p>Meaning that the Extension:</p><p>- will use a Databricks configuration profile named `db-notebook`</p><p>- will activate the Python virtual environment named `dbconnect_env`</p></pre> |

- Open a Databricks notebook (e.g. `notebooks/sample.py`), it should be parsed and displayed as a notebook. Run All or run individual cells
  - some magics, like `%sh`, allow execution on the local machine or a remote Databricks cluster by selecting the relevant cell language

<sup>\*</sup>Additinal settings which can also be set, but are not required:

| Setting                     | Description                               | Default Value |
| :-------------------------- | :---------------------------------------- | :------------ |
| `dbNotebook.showPythonRepl` | Show/hide the Python REPL (true or false) | `false`       |

## An example of configuring Databricks Connect

> 📘 **Note**: this is just a shorter extract from [Databricks Connect reference](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html)

1. Assure that [Databricks Connect requirements](https://docs.databricks.com/dev-tools/databricks-connect-ref.html#requirements) requirements are met (and a compliant Python environment is available for use)
2. [Install the Databricks Connect client](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html#step-1-install-the-databricks-connect-client)
3. Configure properties to establish a connection between Databricks Connect and your remote Databricks cluster. There are few options available, this is an example of using [Databricks configuration profiles](https://docs.databricks.com/en/dev-tools/auth.html#config-profiles)

   - create a `.databrickscfg` configuration file containing a named profile as described in [Databricks configuration profiles](https://docs.databricks.com/en/dev-tools/auth.html#config-profiles)
   - in addition to the `host` and `token` configuration fields, also add a field named `cluster_id` with the value being the Databricks cluster ID

   For example, your `.databrickscfg` configuration file might look like this

   ```
   [my-profile-name]
   host = https://dbc-a1b2c3d4-e4f5.cloud.databricks.com
   token = dapi0123456789
   cluster_id = 0123-456789-0abcdefg
   ```

4. use the name of the created configuration profile (e.g. `my-profile-name` in the example above) to be used in the `dbNotebook.pythonEnvActivationCommand` setting of the Extension [settings.json](https://code.visualstudio.com/docs/getstarted/settings#_settingsjson) of the Visual Studio Code

## Roadmap

Significant roadmap features are:

- Support for `%scala` magic
- Support for various display options (e.g. table, image, HTML, etc)
