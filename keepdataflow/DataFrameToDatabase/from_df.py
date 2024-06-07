from keepdataflow.DataFrameToDatabase.FullRefresh import FullRefresh
from keepdataflow.DataFrameToDatabase.merge import Merge


class DataFrameToDatabase(Merge, FullRefresh):
    def __init__(self, _session) -> None:
        self._session = _session
        self.session = self._session()

    #     self.target_table=target_table,
    #     self.column_select =column_select ,
    #     self.source_df = source_df
    #     self.database_url = database_url
    #     self.db_engine = create_engine(self.database_url)
    #     self.Session = sessionmaker(bind=self.db_engine)

    # def from_df(
    #     self,
    #     source_dataframe: DataFrame,  # cls or self
    #     target_table: str,
    #     # database_url: str, #cls
    #     database_module: str,
    #     target_schema: str = None,
    #     batch_size: int = 1000,
    #     select_column: list = None,
    #     temp_type=None,
