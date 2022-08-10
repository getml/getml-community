import numpy as np


def _add_original_columns(original_df, df_selected, cutoff=0):
    cutoff = max(0, cutoff)
    for colname in original_df.columns:
        df_selected[colname] = np.asarray(original_df[colname])[cutoff:]
    return df_selected
