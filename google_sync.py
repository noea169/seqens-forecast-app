# Imports nécessaires
from google.oauth2.service_account import Credentials
import gspread
import os

def connect_to_google_sheets():
    """
    Établit la connexion avec l'API Google Sheets.

    Returns:
        gspread.Client: Client Google Sheets authentifié
    """
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        credentials_path = 'google_credentials.json'
        if not os.path.exists(credentials_path):
            raise FileNotFoundError("Le fichier 'google_credentials.json' est introuvable.")

        credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=scopes
        )
        client = gspread.authorize(credentials)
        return client

    except Exception as e:
        print(f"❌ Erreur de connexion à Google Sheets : {str(e)}")
        raise RuntimeError(f"Échec de la connexion à Google Sheets : {str(e)}")

def add_forecast_row_to_sheet(row_values, worksheet_name=None):
    """
    Ajoute une ligne de données dans la feuille 'Prévisions Collaboratives'.

    Args:
        row_values (list): Liste des valeurs à ajouter
        worksheet_name (str, optional): Nom de l'onglet à cibler (par défaut : la première feuille)

    Returns:
        bool: True si succès, False sinon
    """
    try:
        client = connect_to_google_sheets()

        spreadsheet = client.open("Prévisions Collaboratives")
        sheet = spreadsheet.worksheet(worksheet_name) if worksheet_name else spreadsheet.sheet1

        sheet.append_row(
            row_values,
            value_input_option='USER_ENTERED'
        )

        print("✅ Ligne ajoutée avec succès dans Google Sheets.")
        return True

    except Exception as e:
        print(f"❌ Erreur lors de l'ajout dans Google Sheets : {str(e)}")
        return False
