"""Mock model used for predict_salary_of_job.

Replace with actual model import and logic.
"""
import numpy as np

def predict_salary_of_job(data_df):
    """Expect a pandas DataFrame, return a scalar predicted salary.
    This mock returns (mean of min_salary if present) or a constant fallback.
    """
    try:
        if 'min_salary' in data_df.columns and data_df['min_salary'].notna().any():
            return float(data_df['min_salary'].mean())
    except Exception:
        pass
    # fallback
    return 50000.0
