# Databricks Notebook Runner Extension for Visual Studio Code

[![CI](https://github.com/r3stl355/databricks-notebook-runner/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/r3stl355/databricks-notebook-runner/actions/workflows/main.yml)

The Databricks Notebook Runner Extension for Visual Studio Code allows you to run Databricks notebooks as interactive notebooks in Visual Studio Code.

> 📘 **Notes**:
>
> - This project is Experimental.
> - This project is **not** developed or supported by Databricks. The word `Databricks` in the name is used to merely classify the type of notebooks it can run.
> - A word **Extension** is used in this document to abbreviate, and has the same meaning as **Databricks Notebook Runner Extension**.

## Features

- Run Databricks notebooks on the local development machine in Visual Studio Code without a conversion
- Support for hybrid execution - on the local machine or a remote Databricks cluster
- The following Databricks-specific variables are instantiated through Databricks Connect integration and available by default. They are expected to work in the same way they do when a Databricks notebook is executed on the Databricks Platform:

  - `spark`, an instance of `databricks.connect.DatabricksSession`
  - `dbutils`, an instance of Databricks Utilities (`dbutils`)

- The following notebook magics are supported:
  - `%frun`: runs a target Python file in the context of the current notebook. It can be any Python file, not necessarily a Databricks notebook
  - `%fs`: runs `dbutils.fs`
  - `%sh`: runs a given Shell command on the local machine, or on the Databricks cluster if the `Remote Shell` cell language is selected
  - `%md`: parses and displays the cell content as Markdown
  - `%sql`: runs the cell content as an SQL statement using `spark.sql`
  - `%pip`: runs `pip` command on the local machine

## Limitations

- The `%run` and `%fs` magics are expected to be single-line command with only one of them per cell
- Any magic which is not listed in the Features section (e.g. `%scala` magic) is not supported and will be treated as Text
- Databricks notebooks are Python files with `.py` file extension (with some special comments to provide additional structure), which means that the Extension will attempt to parse any Python file. If the file is not a valid Databricks Notebook then it will be parsed as a notebook with a single cell. To use a default VSCode parser for those files you may need to disable the Databricks Notebook Runner Extension.
- The Extension was developed and tested on MacOS. There might be some unexpected surprises on other operating systems (please capture those, or any other problems by creating GitHub issues)

## Requirements

- A Databricks Workspace with
  - a pre-configured Databricks cluster running Databricks Runtime 13.0 or higher
  - a Databricks personal access token (PAT) for a user who has access to the Databricks cluster
- Visual Studio Code 1.80.2 or higher (the Extension may still work with the earlier versions but that was tested)
- Visual Studio Code configured to work with Python
- A Python environment (Python virtual environment is recommended but not enforced) with:
- Databricks Connect installed and configured. For more information see [Databricks Connect reference](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html)

## Quick start

- Install and configure Databricks Connect as described in [Databricks Connect reference](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html). There are a few ways to configure Databricks Connect, see [An example of configuring Databricks Connect](#an-example-of-configuring-databricks-connect) section down below for a complete example.
- In Visual Studio Code, search for `Databricks Notebook Runner` via the `Search Extensions in Marketplace` (accessible through the `Extensions` button in the main `Activity` panel) and install the extension.
- Open the folder containing Databricks notebooks. A pair of sample notebooks are included in this repository under `notebooks`
- Configure the Extension. Add the following settings<sup>\*</sup> to Visual Studio Code (or just configure your environment to match the default values). For more information see [Extension settings](https://code.visualstudio.com/docs/getstarted/settings#_extension-settings):

| Setting                                 | Description                                                                           | Default Value                                                                                                                                                                                                                                                                                 |
| :-------------------------------------- | :------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dbNotebook.pythonEnvActivationCommand` | Custom Python environment activation command (must have Databricks Connect installed) | `export DATABRICKS_CONFIG_PROFILE=db-notebook-runner && workon db_connect_env`</code><pre><p>Meaning that the Extension:</p><p>- will use a Databricks configuration profile named `db-notebook-runner`</p><p>- will activate the Python virtual environment named `db_connect_env`</p></pre> |

- Open a Databricks notebook (e.g. `notebooks/sample.py`), it should be parsed and displayed as a notebook. Run All or run individual cells

> 📘 **Note**: Some magics, like `%sh`, allow execution on the local machine or a remote Databricks cluster by selecting the relevant cell language. However, the non-default language selection may not persist on reload

<sup>\*</sup>Additinal settings which can also be set, but are not required:

| Setting                     | Description                               | Default Value |
| :-------------------------- | :---------------------------------------- | :------------ |
| `dbNotebook.showPythonRepl` | Show/hide the Python REPL (true or false) | `false`       |

## An example of configuring Databricks Connect

> 📘 **Note**: This example assumes that your Databricks cluster is using Databricks Runtime 13.2, which requires Python 3.10. Adjust the scripts if using a different Databricks Runtime.

1. Assure that [Databricks Connect requirements](https://docs.databricks.com/dev-tools/databricks-connect-ref.html#requirements) requirements are met (and a compliant Python environment is available for use)

   - Install [virtualenvwrapper](http://virtualenvwrapper.readthedocs.org) for easy Python virtual environment management
   - Create a dedicated Python virtual environment for Databricks Connect

   ```bash
   mkvirtualenv -p python3.10 db_connect_env
   ```

2. [Install the Databricks Connect client](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html#step-1-install-the-databricks-connect-client)

```
# activate the environment if not active
# workon db_connect_env
pip install --upgrade "databricks-connect==13.2pip install --upgrade "databricks-connect==13.1.*".*"
```

3. Configure properties to establish a connection between Databricks Connect and your remote Databricks cluster. There are few options available, this is an example of using [Databricks configuration profiles](https://docs.databricks.com/en/dev-tools/auth.html#config-profiles)

   - create a `.databrickscfg` configuration file in `~` (or `%USERPROFILE%`on Windows) containing a profile named `db-notebook-runner` (or any other name of your choice. `db-notebook-runner` is used by the Extention by default which saves you from configuring it for a quicker start) as described in [Databricks configuration profiles](https://docs.databricks.com/en/dev-tools/auth.html#config-profiles)
   - in addition to the `host` and `token` configuration fields, also add a field named `cluster_id` with the value being the Databricks cluster ID

   Here is an example of what the contents of `.databrickscfg` file should look like

   ```conf
   [db-notebook-runner]
   host = https://dbc-a1b2c3d4-e4f5.cloud.databricks.com
   token = dapi0123456789
   cluster_id = 0123-456789-0abcdefg
   ```

4. Test the Databricks Connect configuration

- Assure that Databricks cluster is running (though running these commands against the Databricks cluster in stopped state will start the cluster, it will take longer to get the results and the command may need to be repeated few times while the cluster is in `pending` state)
- Modify the following command using the values used to create configuration in `.databrickscfg` file to launch `pyspark` (used the `token` and `cluster_id` as is but for `<workspace-instance-name>`, use the `host` value from the `.databrickscfg` without the `https://` prefix)

  ```bash
  WORKSPACE=<value of `host` without `https://`>
  TOKEN=<`token` value>
  CLUSTER_ID=<`cluster_id` value>

  pyspark --remote "sc://$WORKSPACE:443/;token=$TOKEN;x-databricks-cluster-id=$CLUSTER_ID"
  ```

- wait for the `>>>` Spark prompt then run the following command. You should see a table with 10 rows in few seconds

  ```python
  spark.range(10).show()
  ```

## Troubleshooting

1. If you encounter a problem with the extension, or see a notification suggesting to reload the extension, open a Command Palette (e.g. `Shift`+`Command`+`P` on MacOS) and search for `Reload Window`.
   > **Note:** this will start a new session which means that the existing context will be cleared so you may have to re-run some of the previously executed cells.
2. To see what is happening in the REPL, set `dbNotebook.showPythonRepl` to `true` in the Settings. REPL output is sent to a temporary file so you'll also need to locate that file and inspect the contents to see the full output.

## Roadmap

Significant roadmap features are:

- Support for `%scala` magic
- Support for richer display options (e.g. table, image, HTML, etc)
- Support for UI-based configuration (e.g. select a cluster, etc)
