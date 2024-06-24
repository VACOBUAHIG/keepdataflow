from sqlalchemy import (
    Column,
    MetaData,
    Table,
    create_engine,
    inspect,
)
from sqlalchemy.schema import CreateTable

# Define the source and destination database URIs
source_db_uri = 'mysql+pymysql://username:password@localhost/source_db'

# Create engine for the source database
source_engine = create_engine(source_db_uri)

# Use inspector to get table schema
inspector = inspect(source_engine)
columns_info = inspector.get_columns('source_table_name')
primary_key_info = inspector.get_pk_constraint('source_table_name')

# Define the metadata
metadata = MetaData()

# Define the new schema and table name
new_schema = 'new_schema'
new_table_name = 'new_table_name'

# Create a new table object with a different name and schema using the inspector data
columns = [
    Column(col['name'], col['type'], primary_key=(col['name'] in primary_key_info['constrained_columns']))
    for col in columns_info
]
new_table = Table(new_table_name, metadata, *columns, schema=new_schema)

# Generate the DDL for creating the new table
create_table_ddl = CreateTable(new_table).compile(source_engine)

# Output the DDL and primary key information
print(f"Table DDL for '{new_schema}.{new_table_name}':\n{create_table_ddl}")
print(f"Primary Key:\n{primary_key_info}")
