import numpy as np
import pandas as pd

def mape(y_true, y_pred):
    """
    Calcule le Mean Absolute Percentage Error (MAPE)
    entre deux séries de valeurs.
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    # Masque : éviter division par zéro ou valeurs manquantes
    mask = (y_true != 0) & (~np.isnan(y_true)) & (~np.isnan(y_pred))

    if not np.any(mask):
        return np.nan  # Aucun point valide

    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
