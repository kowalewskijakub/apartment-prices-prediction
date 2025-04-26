import pandas as pd
from kedro.pipeline import node, Pipeline

def concatenate_apartments(apartments_partitioned):
    """Load and concatenate all CSV partitions, adding a 'month' column."""
    dfs = []
    for partition_id, load_func in apartments_partitioned.items():
        df = load_func()  # Load the CSV data into a DataFrame
        # Extract month from the file name (e.g., '2023_08' from 'apartments_pl_2023_08.csv')
        month = partition_id.split('_')[-1].replace('.csv', '')
        year = partition_id.split('_')[-2].replace('.csv', '')
        df['month'] = month
        df['year'] = year
        dfs.append(df)
    # Concatenate all DataFrames, resetting the index
    concatenated_df = pd.concat(dfs, ignore_index=True)
    return concatenated_df

# Define the Kedro node
concatenate_node = node(
    func=concatenate_apartments,
    inputs="apartments_partitioned",
    outputs="concatenated_apartments",
    name="concatenate_apartments_node"
)

def create_pipeline(**kwargs):
    return Pipeline([concatenate_node])