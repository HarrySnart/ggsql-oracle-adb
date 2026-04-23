"""Create a density plot of AMOUNT by REASON using ggsqlOracle.

This script follows the ggsql‑oracle‑skill workflow:
1. Load Oracle credentials from a local `config.json` file (same format as the
   example script `oracle_violin.py`).
2. Connect to the Oracle database.
3. Run a validated SQL query that pulls the HMEQ table.
4. Use a ggsql query that draws a density chart of `AMOUNT` coloured by `REASON`.
5. Save the resulting Altair chart as a PNG file `density_agent_plot.png`.

Make sure the `config.json` file exists alongside this script and contains
the necessary connection details. The script can be executed with:
    python3 density_plot.py
"""

from __future__ import annotations

import json
from pathlib import Path

import oracledb

from ggsqlOracle import ggsqlOracle

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
# Path to a JSON file containing the Oracle connection details.
# The file should have the structure:
# {
#   "user": ["YOUR_USERNAME"],
#   "password": ["YOUR_PASSWORD"],
#   "dsn": ["YOUR_DSN"],
#   "config_dir": ["PATH_TO_ORACLE_CONFIG_DIR"],
#   "wallet_dir": ["PATH_TO_WALLET_DIRECTORY"],
#   "wallet": ["WALLET_PASSWORD"]
# }
CONFIG_PATH = Path("./config.json")

# ----------------------------------------------------------------------
# SQL & ggsql queries
# ----------------------------------------------------------------------
# Simple SELECT that returns the columns we need for the plot.
SQL_STATEMENT = """
SELECT value AS amount, reason
FROM hmeq
""".strip()

# ggsql query that builds a density chart.
# - `VISUALISE amount AS y, reason AS color` maps the numeric column to the Y‑axis
#   and uses `reason` to colour the different density curves.
# - `DRAW density` tells ggsql / Altair to render a density plot.
# - `LABEL` supplies a title for the visualization.
GGSQL_QUERY = """
VISUALISE amount AS y, reason AS color
DRAW density
LABEL title => 'Density of AMOUNT by REASON'
""".strip()

# ----------------------------------------------------------------------
# Helper to read the JSON credentials file.
# ----------------------------------------------------------------------
def load_credentials(config_path: Path) -> dict[str, str]:
    """Load Oracle connection details from the supplied JSON file.

    The configuration file may be either:
    1. A dictionary where each value is a list containing a single string
       (as used in the example script), or
    2. A list containing a single dictionary with plain string values
       (as found in the repository's `config.json`).

    This function normalises both formats to a simple dict of strings.
    """
    raw = json.loads(config_path.read_text())
    # If the file contains a list, take the first element.
    if isinstance(raw, list):
        raw = raw[0]

    # If values are lists (e.g., ["value"]), extract the first element.
    result: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(value, list):
            result[key] = value[0]
        else:
            result[key] = value
    return result


def main() -> None:
    # Load credentials.
    creds = load_credentials(CONFIG_PATH)

    # Establish Oracle connection.
    connection = oracledb.connect(
        user=creds["user"],
        password=creds["password"],
        dsn=creds["dsn"],
        config_dir=creds["config_dir"],
        wallet_location=creds["wallet_dir"],
        wallet_password=creds["wallet"],
    )

    # Initialise the ggsqlOracle helper.
    client = ggsqlOracle(connection)

    # Execute the validated SQL statement and retrieve a Polars DataFrame.
    df = client.sync(sql=SQL_STATEMENT)
    print(f"Fetched {len(df)} rows from HMEQ")

    # Render the Altair chart using the ggsql query.
    chart = client.plot(GGSQL_QUERY)

    # Save the chart as a PNG file.
    output_path = Path("./density_agent_plot.png")
    chart.save(output_path)
    print(f"Saved density plot to {output_path.resolve()}")


if __name__ == "__main__":
    main()
