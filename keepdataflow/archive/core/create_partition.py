from typing import (
    List,
    Optional,
    Union,
)

import polars as pl


def partition_dataframe(
    df: pl.DataFrame, column_name: Optional[str] = None, chunk_size: int = 5000, as_dict: bool = False
) -> Union[List[pl.DataFrame], dict[int, pl.DataFrame]]:
    """
    Partition a DataFrame either by a specific column or by chunk size.

    Args:
        df (pl.DataFrame): The DataFrame to partition.
        column_name (Optional[str]): The column name to partition by. If None, partition by chunk size.
        chunk_size (int): The size of each chunk if partitioning by chunk size. Default is 5000.
        as_dict (bool): Whether to return the partitions as a dictionary with indices as keys. Default is False.

    Returns:
        Union[List[pl.DataFrame], dict[int, pl.DataFrame]]: The partitioned DataFrame as a list or dictionary.
    """
    if column_name in df.columns:
        # Partition by column
        return df.partition_by(column_name, as_dict=False)

    # Create an empty list to store the partitions
    partitions = []

    # Loop over the DataFrame in chunks of the specified size
    for i in range(0, len(df), chunk_size):
        partitions.append(df.slice(i, chunk_size))

    if as_dict:
        # Create a dictionary with indices as keys
        partition_dict = {i: partition for i, partition in enumerate(partitions)}
        return partition_dict

    return partitions
