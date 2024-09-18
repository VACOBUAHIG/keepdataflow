import json
from sqlalchemy import create_engine
from data_transfer import DatabaseDataTransfer


def load_config(file_path):
    """Load the configuration file and return it as a dictionary."""
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config


def run_transfers(config):
    """
    Run data transfers based on the configuration provided in the config file.

    The configuration should include:
    - source and destination database connection strings.
    - the type of operation (MERGE, INSERT ON CONFLICT, or basic INSERT).
    """
    # Create SQLAlchemy engines for the source and destination databases
    source_connection_string = config["database"]["source_connection_string"]
    destination_connection_string = config["database"]["destination_connection_string"]

    # Create engines using SQLAlchemy
    source_engine = create_engine(source_connection_string)
    destination_engine = create_engine(destination_connection_string)

    # Initialize the DatabaseDataTransfer object (db_type is inferred automatically)
    data_transfer = DatabaseDataTransfer(
        source_engine=source_engine,
        destination_engine=destination_engine,
        operation=config["database"].get("operation", "merge"),  # Default operation is merge
    )

    # Transfer tables listed in the configuration
    for table_config in config["tables"]:
        source_table = table_config["source_table"]
        destination_table = table_config["destination_table"]
        source_schema = table_config.get("source_schema", "dbo")  # Default schema is dbo
        destination_schema = table_config.get("destination_schema", "dbo")  # Default schema is dbo

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

        # Perform the data transfer
        data_transfer.transfer(
            source_table=source_table,
            destination_table=destination_table,
            source_schema=source_schema,
            destination_schema=destination_schema,
            **additional_args,
        )
