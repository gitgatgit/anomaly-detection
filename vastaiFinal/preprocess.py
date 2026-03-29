import pandas as pd
import numpy as np

def engineer_features(df):
    """
    Apply business logic and derive new columns from the raw BigQuery dataset.
    This should be run after loading the downcasted .feather files.
    """
    # Handle NaNs from BigQuery Left/Outer joins
    if 'instigator_net_token_profit' in df.columns:
        df['instigator_net_token_profit'] = df['instigator_net_token_profit'].fillna(0.0)
    if 'instigator_fiat_delta' in df.columns:
        df['instigator_fiat_delta'] = df['instigator_fiat_delta'].fillna(0.0)
    if 'instigator_sol_delta' in df.columns:
        df['instigator_sol_delta'] = df['instigator_sol_delta'].fillna(0.0)
    
    # Composability ratios (all programs: known + unknown)
    if 'unique_program_count' in df.columns and 'unique_nonsigner_account_count' in df.columns:
        df['hop_density'] = (df['unique_program_count'] / df['unique_nonsigner_account_count'].replace(0, np.nan)).fillna(0.0)
    
    if 'max_cpi_depth' in df.columns and 'unique_program_count' in df.columns:
        df['avg_depth_per_protocol'] = (df['max_cpi_depth'] / df['unique_program_count'].replace(0, np.nan)).fillna(0.0)
    
    # Novel contract count
    if all(c in df.columns for c in ['unique_program_count', 'dex_hop_count', 'debt_hop_count']):
        df['unknown_program_count'] = (df['unique_program_count'] - df['dex_hop_count'] - df['debt_hop_count']).clip(lower=0)
    
    # Profit flags
    if 'instigator_fiat_delta' in df.columns:
        df['has_fiat_profit'] = (df['instigator_fiat_delta'] > 0).astype(int)
    if 'instigator_net_token_profit' in df.columns:
        df['has_token_profit'] = (df['instigator_net_token_profit'] > 0).astype(int)
    if 'instigator_sol_delta' in df.columns:
        df['has_sol_profit'] = (df['instigator_sol_delta'] > 0).astype(int)
        
    return df
