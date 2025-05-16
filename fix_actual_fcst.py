"""
Script pour corriger le problème 'list' object has no attribute 'values'
dans la fonction add_variance_indicators
"""

def safe_float(val):
    """Convertit une valeur en float de manière sécurisée"""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def add_variance_indicators_fixed(df):
    """
    Version corrigée de la fonction add_variance_indicators qui traite correctement
    les listes et les dictionnaires dans actual_fcst
    """
    df['variance_indicator'] = ''

    for idx, row in df.iterrows():
        forecast_val = safe_float(row.get('value'))
        budget_val = safe_float(row.get('full_year_budget'))

        # Vérification écart prévision vs budget
        if forecast_val is not None and budget_val is not None:
            if check_variance(forecast_val, budget_val):
                df.at[idx, 'variance_indicator'] += '⚠️ Écart budget significatif\n'

        # Traitement de actual_fcst
        hist_raw = row.get('actual_fcst')
        
        # Initialiser hist_values comme une liste vide
        hist_values = []
        
        # Si c'est une liste, extraire directement les valeurs
        if isinstance(hist_raw, list):
            hist_values = [
                safe_float(v)
                for v in hist_raw
                if safe_float(v) is not None
            ]
        # Si c'est un dict, récupérer ses valeurs
        elif isinstance(hist_raw, dict):
            hist_values = [
                safe_float(v)
                for v in hist_raw.values()
                if safe_float(v) is not None
            ]

        # Vérification écart prévision vs historique
        if hist_values:
            avg_hist = sum(hist_values) / len(hist_values)
            if forecast_val is not None and check_variance(forecast_val, avg_hist):
                df.at[idx, 'variance_indicator'] += '📊 Écart historique significatif\n'

    return df

def check_variance(value1, value2):
    """
    Vérifie si l'écart relatif entre deux valeurs dépasse le seuil défini.
    La base de calcul est la moyenne des deux pour éviter les distorsions.
    """
    try:
        v1 = float(value1)
        v2 = float(value2)
    except (TypeError, ValueError):
        return False

    if v1 == 0 and v2 == 0:
        return False  # aucun écart s'il n'y a rien des deux côtés

    baseline = (abs(v1) + abs(v2)) / 2
    if baseline == 0:
        return False  # éviter toute division par 0

    relative_diff = abs(v1 - v2) / baseline
    VARIANCE_THRESHOLD = 0.3  # 30% d'écart
    return relative_diff > VARIANCE_THRESHOLD