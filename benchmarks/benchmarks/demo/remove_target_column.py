import numpy as np
import pandas as pd


def _remove_target_column(data_frame: pd.DataFrame, target: str) -> pd.DataFrame:
    colnames = np.asarray(data_frame.columns)
    if target not in colnames:
        return data_frame
    colnames = colnames[colnames != target]
    return data_frame[list(colnames)]
