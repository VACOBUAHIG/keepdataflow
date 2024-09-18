from data_transfer import DatabaseDataTransfer

from sqlalchemy import create_engine


sourceServerAddress = "VHAAUSDB2.VHA.MED.VA.GOV"
sourceDatabaseName = "CapAssets_Prod"

destinationserverAddress = "VHACDWA01.VHA.MED.VA.GOV"
destinationDatabaseName = "HEFP_EHRMSPCAM"

source_engine = create_engine(
    f"mssql+pyodbc://{sourceServerAddress}/{sourceDatabaseName}?driver=ODBC+Driver+17+for+SQL+Server"
)
destination_engine = create_engine(
    f"mssql+pyodbc://{destinationserverAddress}/{destinationDatabaseName}?driver=ODBC+Driver+17+for+SQL+Server"
)


# from sqlalchemy.engine.url import make_url


# url = make_url(source_engine)

# dialect = url.get_dialect().name
# print(dialect)

data_transfer = DatabaseDataTransfer(source_engine=source_engine, destination_engine=destination_engine)
data_transfer.transfer(
    source_table='aspnet_Applications',
    destination_table='aspnet_Applications',
    source_schema='dbo',
    destination_schema='CAP',
    operation="insert",
)

# def sample(source_table, source_schema=None):
#     if source_schema:
#         source_table_spec = f"{source_schema}.{source_table}"
#     else:
#         source_table_spec = f"{source_table}"

#     print(source_table_spec)


# sample(source_table='users', source_schema='VHA')
