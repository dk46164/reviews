
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import os
import logging
import re
from dask.distributed import print as dd_print
import numpy as np

def load_inappropriate_words(root_path):
    """
    Load inappropriate words from.txt files in the given root path.
    
    Args:
        root_path (str): Root path to search for.txt files containing inappropriate words.
    
    Returns:
        set: A set of inappropriate words.
    """
    # Get logger
    logger = logging.getLogger('dask_scheduler_logger')

    # Initialize set of inappropriate words
    inappropriate_words = set()
    
    # logger
    logger.info(f"Loading inappropriate words from {root_path}")

    for root, dirs, files in os.walk(root_path):
        for file in files:
            if file.endswith('.txt'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    inappropriate_words.update(word.strip().lower() for word in f)
    # logger
    logger.info(f"Loading inappropriate words from {root_path},sample words : {list(inappropriate_words)[:5]}")
    
    return inappropriate_words

def cover_inappropriate_words(text, inappropriate_words):
    """
    Replace inappropriate words in the text with asterisks, including substrings.
    
    Args:
        text (str): The input text.
        inappropriate_words (set): Set of inappropriate words.
    
    Returns:
        str: The text with inappropriate words replaced by asterisks.
    """
    # Convert text and inappropriate words to lowercase
    lower_text = text.lower()
    lower_inappropriate_words = {word.lower() for word in inappropriate_words}
    
    # Create a translation table
    trans_table = str.maketrans({chr(i): '*' for i in range(256)})
    
    # Process the text
    result = []
    i = 0
    while i < len(lower_text):
        for word in lower_inappropriate_words:
            if lower_text.startswith(word, i):
                result.append(text[i:i+len(word)].translate(trans_table))
                i += len(word)
                break
        else:
            result.append(text[i])
            i += 1
    
    censored_text = ''.join(result)
    
    return censored_text
  

def is_review_appropriate(text, inappropriate_words):
    """
    Check if a review is appropriate based on the percentage of inappropriate words.
    
    Args:
        text (str): The review text.
        inappropriate_words (set): Set of inappropriate words.
    
    Returns:
        bool: True if the review is appropriate, False otherwise.
    """
    words = re.findall(r'\b\w+\b', text.lower())
    inappropriate_count = sum(1 for word in words if word in inappropriate_words)
    return (inappropriate_count / len(words)) < 0.2

def process_partition(df, inappropriate_words, current_date):
    """
    Process a partition of the reviews DataFrame.
    Args:
        df (pandas.DataFrame): A partition of the reviews DataFrame.
        inappropriate_words (set): Set of inappropriate words.
        current_date (datetime or str): The current date for age calculations.
    Returns:
        pandas.DataFrame: Processed partition of reviews.
    """
    # Store original columns
    original_columns = df.columns.tolist()

    # Convert publishedAt to datetime and ensure it's tz-aware
    df['publishedAt'] = pd.to_datetime(df['publishedAt'], format='mixed', utc=True)
    
    # Ensure publishedAt is tz-aware
    if df['publishedAt'].dt.tz is None:
        df['publishedAt'] = df['publishedAt'].dt.tz_localize('UTC')
    
    # Calculate age in days
    age = (current_date - df['publishedAt']).dt.total_seconds() / (24 * 60 * 60)
    
    # Create is_outdated mask
    is_outdated = age > (3 * 365)  # More than 3 years old
    
    # Apply inappropriate words filter
    is_appropriate = df['text'].apply(is_review_appropriate, args=(inappropriate_words,))
    
    # Filter the DataFrame
    df = df[~is_outdated & is_appropriate].copy()
    
    # Apply covered_text only to the rows that passed the filter
    df['text'] = df['text'].apply(cover_inappropriate_words, args=(inappropriate_words,))
    
    # Add age column
    df['age'] = age

    # Return only the original columns
    return df[original_columns+[ 'age']]

def mean_text_length(s):
    """
    Calculates the sum of the lengths of all strings in the given Series.

    Parameters:
        s (pandas.Series): A Series containing strings.

    Returns:
        int: The sum of the lengths of all strings in the Series.
    """
    total_length = s.apply(len).sum()
    count = s.count()
    # return total_length / count if count > 0 else 0
    return s.str.len().mean()

# Flatten column names using Dask's map_partitions
def flatten_columns(df):
    df.columns =  [
        'restaurant_id', 'review_count', 'average_score', 'average_review_length',
        'oldest_review_age', 'newest_review_age', 'average_review_age'
    ]
    return df



def aggregate_reviews(ddf):
    """
    Aggregate reviews by restaurant ID.
    
    Args:
        ddf (dask.dataframe.DataFrame): Dask DataFrame of processed reviews.
    
    Returns:
        dask.dataframe.DataFrame: Aggregated reviews.
    """
    return ddf.groupby('restaurantId').agg({
            'reviewId': 'count',
            'rating': 'mean',
            'text': dd.Aggregation(
                        'mean_length',
                        lambda s: s.apply(len).sum(),  # Initial state: sum of lengths
                        lambda x: x.sum(),           # Combine intermediate sums
                        lambda x: x.sum() // x.count() # Finalize: calculate mean
                    ),
            'age': ['min', 'max', 'mean']
                    }).reset_index()


def combine_aggregations(previous, new):
    """
    Combine previous aggregations with new aggregations using Dask operations.
    
    Args:
        previous (dask.dataframe.DataFrame): Previous aggregations
        new (dask.dataframe.DataFrame): New aggregations
    
    Returns:
        dask.dataframe.DataFrame: Combined aggregations
    """

    # Merge previous and new aggregations on restaurant_id
    combined = previous.merge(new, on='restaurant_id', suffixes=('_prev', '_new'), how='outer')
    
    # Calculate combined metrics
    combined['review_count'] = combined['review_count_prev'].fillna(0) + combined['review_count_new'].fillna(0)
    
    # Corrected average score calculation
    combined['average_score'] = (
        (combined['average_score_prev'] * combined['review_count_prev'].fillna(0) + 
         combined['average_score_new'] * combined['review_count_new'].fillna(0)) / 
        combined['review_count']
    ).fillna(combined['average_score_new'])
    
    combined['average_review_length'] = (
        (combined['average_review_length_prev'] * combined['review_count_prev'].fillna(0) + 
         combined['average_review_length_new'] * combined['review_count_new'].fillna(0)) / 
        combined['review_count']
    ).fillna(combined['average_review_length_new'])
    
    combined['oldest_review_age'] = combined[['oldest_review_age_prev', 'oldest_review_age_new']].max(axis=1)
    combined['newest_review_age'] = combined[['newest_review_age_prev', 'newest_review_age_new']].min(axis=1)
    
    combined['average_review_age'] = (
        (combined['average_review_age_prev'] * combined['review_count_prev'].fillna(0) + 
         combined['average_review_age_new'] * combined['review_count_new'].fillna(0)) / 
        combined['review_count']
    ).fillna(combined['average_review_age_new'])
    
    # Select final columns
    final_columns =  [
        'restaurant_id', 'review_count', 'average_score', 'average_review_length',
        'oldest_review_age', 'newest_review_age', 'average_review_age'
    ]
    
    return combined[final_columns]



def process_reviews(client,reviews_ddf, inappropriate_words_file, output_file, aggregations_file,logger):
    """
    Process reviews, filter inappropriate content, and generate aggregations.
    
    Args:
        reviews_ddf (dask.dataframe.DataFrame): Dask DataFrame of reviews.
        inappropriate_words_file (str): Path to file containing inappropriate words.
        output_file (str): Path to output file for processed reviews.
        aggregations_file (str): Path to output file for aggregations.

    Returns:
        dask.dataframe.DataFrame: Processed reviews.
    """
    # logger
    logger.info(f'Processing inappropriate_words file {inappropriate_words_file}')

    # Load inappropriate words
    inappropriate_words = client.submit(load_inappropriate_words, inappropriate_words_file).result()
        
    
    # Ensure current_date is a tz-aware pandas Timestamp object
    current_date =pd.Timestamp(datetime.now(), tz='UTC')
    
    # logger
    logger.info(f"Applying Review Text Maksing and Filtering inappropriate words")

    # Process reviews
    processed_reviews = reviews_ddf.map_partitions(
        process_partition,
        inappropriate_words=inappropriate_words,
        current_date=current_date,
        meta = {
            'restaurantId': 'int64',
            'reviewId': 'int64',
            'text': 'object',
            'rating': 'float64',
            'publishedAt': 'datetime64[ns]',
            'source_file': 'object',  # String column for source file
            'parse_error': 'bool',  # Boolean column for parse error
            'age':  'float64'
        })

    # Ensure we have a Dask DataFrame
    if not isinstance(processed_reviews, dd.DataFrame):
        processed_reviews = dd.from_pandas(processed_reviews, npartitions=reviews_ddf.npartitions)

    # Aggregate reviews using Dask
    previous_aggregations = None

    # # Load previous aggregations
    if os.path.exists(os.path.join(aggregations_file,'review_aggregation_join')):
        previous_aggregations = dd.read_json(os.path.join(aggregations_file,'review_aggregation_join'), lines=True)
        logger.info(f"Loaded previous aggregations from {os.path.join(aggregations_file,'review_aggregation_join')}")
    # else:
        previous_aggregations = None
        logger.info(f"No previous aggregations found at {os.path.join(aggregations_file,'review_aggregation_join')}")

    # get aggregations
    agg_reviews = aggregate_reviews(processed_reviews)

    logger.info(f"Reviews Aggregated from processed reviews")

    new_aggregations =  agg_reviews.map_partitions(flatten_columns,meta={
        'restaurant_id': 'int64', 'review_count': 'int64', 'average_score': 'float64', 'average_review_length': 'object', 'oldest_review_age':'float64', 'newest_review_age':'float64', 'average_review_age':'float64'
    })
    
    # Combine new aggregations with previous aggregations
    if previous_aggregations is not None:
        combined_aggregations = combine_aggregations(previous_aggregations, new_aggregations)
    else:
        combined_aggregations = new_aggregations
    
    # logger
    logger.info(f"Aggregations combined from previous and new aggregations")

    combined_aggregations_df = combined_aggregations.to_json(os.path.join(aggregations_file, 'review_aggregations_join/*.part'), lines=True, orient='records')

    # return expected schema to filerename columns
    combined_aggregations_df  = combined_aggregations.compute()
    combined_aggregations_df.columns =  ["restaurantId","reviewCount","averageRating","averageReviewLength","mean","oldest","newest"]
    combined_aggregations_df['averageReviewLength'] = combined_aggregations_df['averageReviewLength'].astype(int)
    combined_aggregations_df['mean'] = combined_aggregations_df['mean'].astype(int)
    combined_aggregations_df['oldest'] = combined_aggregations_df['oldest'].astype(int)
    combined_aggregations_df['newest'] = combined_aggregations_df['newest'].astype(int)
    combined_aggregations_df['reviewCount'] = combined_aggregations_df['reviewCount'].astype(int)
    combined_aggregations_df['restaurantId'] = combined_aggregations_df['restaurantId'].astype(int)

    combined_aggregations_df['ReviewAge'] = combined_aggregations_df[["mean","oldest","newest"]].to_dict(orient='records')
    combined_aggregations_df = combined_aggregations_df[["restaurantId","reviewCount","averageRating","averageReviewLength","ReviewAge"]]


    # Write Aggerated reviews
    combined_aggregations_df.to_json(os.path.join(aggregations_file, 'review_aggregation.json'), lines=True, orient='records')
    logger.info(f"Aggregated reviews written to {os.path.join(aggregations_file, 'review_aggregation.json')}")
    logger.info(f"Combined aggregations flushed to {aggregations_file}")

    return processed_reviews
