import json
from sqlalchemy import create_engine
from data_transfer import DatabaseDataTransfer
from data_engineer_utils import get_execution_order, sort_table_mappings


def load_config(file_path):
    """Load the configuration file and return it as a dictionary."""
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config


def run_transfers(config, source_connection=None, destination_connection=None, enforce_table_sort=False):
    """
    Run data transfers based on the configuration provided in the config file.

    The configuration should include:
    - source and destination database connection strings.
    - the type of operation (MERGE, INSERT ON CONFLICT, or basic INSERT).
    """
    # Create SQLAlchemy engines for the source and destination databases

    if config["database"]["sourceConnectionString"] == "":
        config["database"]["sourceConnectionString"] = source_connection
    if config["database"]["destinationConnectionString"] == "":
        config["database"]["destinationConnectionString"] = destination_connection

    # if config["database"]["sourceConnectionString"] or config["database"]["destinationConnectionString"] is None:
    #     raise ValueError(f'Unsupported operation: {source_connection}')

    source_connection_string = config["database"]["sourceConnectionString"]
    destination_connection_string = config["database"]["destinationConnectionString"]

    # # Create engines using SQLAlchemy
    source_engine = create_engine(source_connection_string)
    destination_engine = create_engine(destination_connection_string)

    # Initialize the DatabaseDataTransfer object (db_type is inferred automatically)
    data_transfer = DatabaseDataTransfer(
        source_engine=source_engine,
        destination_engine=destination_engine,
        # Default operation is merge
    )
    if enforce_table_sort:
        execution_order = get_execution_order(destination_engine)
        table_mapping = config.get('tables')
        tables = sort_table_mappings(table_mapping, execution_order)

    else:
        tables = config.get('tables')
    # Transfer tables listed in the configuration
    for table_config in tables:
        source_table = table_config["sourceTable"]
        destination_table = table_config["targetTable"]
        source_schema = table_config.get("sourceSchema", "dbo")  # Default schema is dbo
        destination_schema = table_config.get("targetSchema", "dbo")  # Default schema is dbo
        operation = table_config.get("operation", "append")

        print(f"Transferring {source_table} from {source_schema} to {destination_table} in {destination_schema}...")

        # Extract additional kwargs if provided
        additional_args = {}
        if "match_columns" in table_config:
            additional_args["match_columns"] = table_config["match_columns"]
        if "conflict_columns" in table_config:
            additional_args["conflict_columns"] = table_config["conflict_columns"]
        if "include_match_condition_in_insert" in table_config:
            additional_args["include_match_condition_in_insert"] = table_config["include_match_condition_in_insert"]
        if "skip_inserts" in table_config:
            additional_args["skip_inserts"] = table_config["skip_inserts"]
        if "skip_updates" in table_config:
            additional_args["skip_updates"] = table_config["skip_updates"]

        #     # Perform the data transfer
        data_transfer.transfer(
            source_table=source_table,
            destination_table=destination_table,
            source_schema=source_schema,
            destination_schema=destination_schema,
            operation=operation,
        )
