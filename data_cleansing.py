# data_cleaning.py

import pandas as pd

# 1.1 Normalize column names: lowercase, strip spaces, replace spaces with underscores
def normalize_columns(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
# 1.2 rename column
    df = df.rename(columns={"gendr": "gender"})
    return df

# 2. Standardize object columns (except client_id)
def standardize_object_columns(df, exclude_cols=None):
    df = df.copy()
    if exclude_cols is None:
        exclude_cols = []
    for col in df.select_dtypes(include='object').columns:
        if col not in exclude_cols:
            df[col] = df[col].str.strip().str.lower()  # strip() → only for object/string columns; lower() → only for object/string columns
    return df

# 3. Ensure client_id is string
def standardize_client_id(df, column='client_id'):  # astype(str) → necessary for client_id if it is used as a key or merge
    if column in df.columns:
        df[column] = df[column].astype(str)
    return df



# 4️. Full cleaning workflow for the three dataframes
def clean_dataframes(web_df, exp_df, demo_df):
    # Normalize column names
    web_df = normalize_columns(web_df)
    exp_df = normalize_columns(exp_df)
    demo_df = normalize_columns(demo_df)
    
    # Standardize object columns (exclude client_id from stripping)
    web_df = standardize_object_columns(web_df, exclude_cols=['client_id'])
    exp_df = standardize_object_columns(exp_df, exclude_cols=['client_id'])
    demo_df = standardize_object_columns(demo_df, exclude_cols=['client_id'])
    
    # Standardize client_id
    web_df = standardize_client_id(web_df)
    exp_df = standardize_client_id(exp_df)
    demo_df = standardize_client_id(demo_df)
    
    return web_df, exp_df, demo_df

# 5️. Merge all three dataframes
def merge_all_data(web_df, exp_df, demo_df):
    """
    Merge web activity, experiment assignment, and demographics
    into a single master dataframe using client_id as primary key.
    """
    # Merge web with experiment assignment
    merged = web_df.merge(
        exp_df[['client_id', 'variation']],
        on='client_id',
        how='left',
        validate='many_to_one'
    )
    
    # Merge the result with demographics
    merged = merged.merge(
        demo_df,
        on='client_id',
        how='left',
        validate='many_to_one'
    )
    
    return merged

