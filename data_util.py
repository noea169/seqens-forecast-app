import pandas as pd
import numpy as np
from datetime import datetime

def load_from_excel(file_path, sheet_name=None):
    """
    Charge les données depuis un fichier Excel.
    
    Args:
        file_path: Chemin vers le fichier Excel
        sheet_name: Nom de la feuille à charger (si None, charge toutes les feuilles)
    
    Returns:
        Tuple (dataframe, success, message)
    """
    try:
        # Si un nom de feuille est spécifié, on charge juste cette feuille
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            # Sinon, on charge toutes les feuilles dans un dictionnaire
            df = pd.read_excel(file_path, sheet_name=None)
        
        # Si tout s'est bien passé, on retourne les données avec un message de succès
        return df, True, "Données chargées avec succès"
    
    except Exception as e:
        # Si une erreur se produit, on retourne None avec un message d'erreur
        return None, False, f"Erreur lors du chargement: {str(e)}"

def preprocess_data(df):
    """
    Nettoie et prépare les données pour l'analyse.
    
    Args:
        df: DataFrame pandas à prétraiter
    
    Returns:
        DataFrame prétraité
    """
    # Faisons une copie pour ne pas modifier l'original
    df_clean = df.copy()
    
    # Conversion des colonnes de dates si elles existent
    for col in df_clean.columns:
        if 'date' in col.lower():
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Gestion des valeurs manquantes dans les colonnes numériques
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    df_clean[numeric_cols] = df_clean[numeric_cols].fillna(0)
    
    # Pour les autres colonnes textuelles, on remplace par "Non spécifié"
    text_cols = df_clean.select_dtypes(include=['object']).columns
    for col in text_cols:
        df_clean[col] = df_clean[col].fillna('Non spécifié')
    
    return df_clean

def restructure_forecast_data(df):
    """
    Restructure les données de prévision Seqens pour l'analyse.
    
    Args:
        df: DataFrame contenant les données brutes de prévision
    
    Returns:
        DataFrame restructuré pour l'analyse
    """
    # Créons une copie pour éviter de modifier l'original
    df_work = df.copy()
    
    # 1. Identifier les types de colonnes
    # Colonnes de base (info client/produit)
    base_columns = []
    time_columns = []
    
    for col in df_work.columns:
        # Si la colonne contient une année (2024, 2025, 2026), c'est une colonne temporelle
        if any(year in str(col) for year in ['2024', '2025', '2026']):
            time_columns.append(col)
        else:
            base_columns.append(col)
    
    # Vérification pour éviter les erreurs
    if not time_columns:
        print("Avertissement: Aucune colonne temporelle trouvée!")
        # Retourner un DataFrame vide avec la colonne 'date' pour éviter l'erreur KeyError
        empty_df = pd.DataFrame({'date': [pd.Timestamp('2024-01-01')]})
        return empty_df
    
    try:
        # 2. Restructurer en format "long" pour l'analyse temporelle
        melted_df = pd.melt(
            df_work,
            id_vars=base_columns,
            value_vars=time_columns,
            var_name='period_type',
            value_name='value'
        )
        
        # 3. Déterminer le type de données (actual, forecast, budget, backlog)
        melted_df['data_type'] = 'Unknown'
        
        # Définir le type de données en fonction du nom de la colonne
        for data_type, keyword in {
            'Actual': ['actual'],
            'Forecast': ['fcst', 'forecast'],
            'Budget': ['budget'],
            'Backlog': ['backlog'],
            'Initial': ['initial']
        }.items():
            for word in keyword:
                mask = melted_df['period_type'].str.lower().str.contains(word, na=False)
                melted_df.loc[mask, 'data_type'] = data_type
        
        # 4. Extraire année et mois de la colonne period_type
        # Recherche pattern comme 2024/01, 2025/02, etc.
        melted_df['year'] = melted_df['period_type'].str.extract(r'(20\d{2})')
        melted_df['month'] = melted_df['period_type'].str.extract(r'\/(\d{2})')
        
        # 5. Créer une date complète pour l'analyse temporelle
        # IMPORTANT: Assurons-nous de créer la colonne 'date' pour éviter l'erreur KeyError
        # Créons d'abord une date par défaut pour éviter les KeyError
        melted_df['date'] = pd.Timestamp('2024-01-01')
        
        # Ensuite, essayons de créer des dates plus précises si possible
        if 'year' in melted_df.columns and 'month' in melted_df.columns:
            # S'assurer qu'il n'y a pas de valeurs None/NaN
            valid_dates = ~(melted_df['year'].isna() | melted_df['month'].isna())
            
            if valid_dates.any():
                # Créer la colonne date seulement pour les lignes avec year et month valides
                melted_df.loc[valid_dates, 'date_str'] = melted_df.loc[valid_dates, 'year'] + '-' + melted_df.loc[valid_dates, 'month'] + '-01'
                date_series = pd.to_datetime(melted_df.loc[valid_dates, 'date_str'], errors='coerce')
                
                # Ne mettre à jour que les dates valides
                valid_dates_after_conversion = ~date_series.isna()
                melted_df.loc[valid_dates_after_conversion.index[valid_dates_after_conversion], 'date'] = date_series[valid_dates_after_conversion]
        
        # 6. Nettoyer les valeurs
        # Convertir la colonne value en numérique si possible
        melted_df['value'] = pd.to_numeric(melted_df['value'], errors='coerce').fillna(0)
        
        # Imprimez les colonnes disponibles dans le DataFrame pour le débogage
        print(f"Colonnes disponibles dans le DataFrame restructuré: {melted_df.columns.tolist()}")
        
        # Vérifiez explicitement que la colonne 'date' existe
        if 'date' not in melted_df.columns:
            print("ERREUR: La colonne 'date' n'a pas été créée correctement!")
            melted_df['date'] = pd.Timestamp('2024-01-01')
        
        return melted_df
    
    except Exception as e:
        print(f"Erreur lors de la restructuration des données: {str(e)}")
        # En cas d'erreur, retourner un DataFrame vide avec la colonne 'date'
        empty_df = pd.DataFrame({'date': [pd.Timestamp('2024-01-01')], 'data_type': ['Unknown'], 'value': [0]})
        return empty_df