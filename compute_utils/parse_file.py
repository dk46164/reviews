import dask.dataframe as dd
from  dask import delayed,compute
import pandas as pd
from dask.distributed import Client
from pydantic import BaseModel,Field
import logging
from typing import List,Tuple
from datetime import datetime

# Pydantic model for data validation
class RestaurantReview(BaseModel):
    restaurantId: int
    reviewId: int
    text: str
    rating: float = Field(ge=0, le=10)  # Rating between 0 and 10
    publishedAt: datetime = Field(format="ISO8601")


def validate_record(row ):
    """
    Validates a record to ensure it meets the required criteria.

    Args:
        record (dict): The record to be validated.

    Returns:
        bool: True if the record is valid, False otherwise.
    """
    # Check if the record has valid keys and values
    if row['parse_error']:
            return False
    try:
        RestaurantReview(**row.to_dict())
        return True
    except Exception as e:
        return False



@delayed
def process_single_file(file_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process a single JSON file and return valid and invalid records.
    """
    # get logger
    logger = logging.getLogger('dask_scheduler_logger')

    try:
        # Read JSON file
        logger.info(f"Reading the file : {file_path}")
        df = pd.read_json(file_path, lines=True,dtype={'restaurantId':int,'reviewId':int,'text':str,'rating':float,'publishedAt':datetime})
        df['source_file'] = file_path

        # Flag to indicate parsing errors
        df['parse_error'] = False   

        # Validate each record
        df['is_valid'] = df.apply(validate_record, axis=1) 
        
        valid_df = df[df['is_valid']].drop('is_valid', axis=1)
        invalid_df = df[~df['is_valid']].drop('is_valid', axis=1)

        return valid_df, invalid_df
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        null_data = {field: [None] for field in RestaurantReview.model_fields.keys()}
        null_data['parse_error'] = True
        null_data['source_file'] = file_path
        return (pd.DataFrame(null_data),pd.DataFrame())

def process_json(client: Client, file_paths: List[str],logger:logging.Logger) -> Tuple[dd.DataFrame, dd.DataFrame]:
    """
    Processes multiple JSON files in parallel, validates their content, and splits into valid and invalid data.
    
    Args:
        client (Client): Dask distributed Client.
        file_paths (List[str]): List of file paths to process.
    
    Returns:
        Tuple[dd.DataFrame, dd.DataFrame]: A tuple containing two Dask DataFrames:
        - valid_df: dask.DataFrame with valid records.
        - invalid_df: daak.DataFrame with invalid records.
    """
    # logger
    logger.info(f"Creating Delayed Objects of Files")

    # Create delayed objects for each file
    delayed_results = [process_single_file(file_path) for file_path in file_paths]

    # logger 
    logger.info(f"Submitting the delayed objects to Dask Client")

    # Compute all results in parallel
    results = compute(*delayed_results)

    # Separate valid and invalid DataFrames
    valid_dfs = [result[0] for result in results]
    invalid_dfs = [result[1] for result in results]

    # Combine results into Dask DataFrames
    valid_df = dd.from_pandas(pd.concat(valid_dfs, ignore_index=True), npartitions=len(file_paths))
    invalid_df = dd.from_pandas(pd.concat(invalid_dfs, ignore_index=True), npartitions=len(file_paths))

    # logger
    logger.info(f"Finished Parsing the review files and performed schema validation")

    # persist the results
    valid_df = client.persist(valid_df, name='valid_data')
    invalid_df = client.persist(invalid_df, name='invalid_data')

    #logger
    logger.info(f"Persisted the valid and invalid dataframes")

    return valid_df, invalid_df


