import streamlit as st
st.set_page_config(
    page_title='Seqens Analytics',
    page_icon='📊',
    layout='wide',
    initial_sidebar_state="collapsed"
    
)

# Imports principaux
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
import tempfile
import io
import base64
from typing import Optional, Dict
import streamlit.components.v1 as components 
import time
import re 

# Imports pour AgGrid
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode

# Imports pour les modèles
from prophet import Prophet
from prophet.plot import plot_plotly
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.preprocessing import StandardScaler

# Imports Streamlit et extensions
import streamlit_authenticator as stauth
from streamlit_extras.colored_header import colored_header
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.add_vertical_space import add_vertical_space
from streamlit_extras.chart_container import chart_container
from streamlit_tags import st_tags
from streamlit_extras.stylable_container import stylable_container

# Autres imports
from dotenv import load_dotenv
import os
import jsonschema



# -----------------------------------------------------------------------------
# GLOBAL CONSTANTS & SCHEMAS
# -----------------------------------------------------------------------------
# Configuration des couleurs et styles
COLORS = {
    'primary': '#1f77b4',
    'success': '#2ecc71',
    'warning': '#f1c40f',
    'danger': '#e74c3c',
    'info': '#3498db'
}

# Tooltips pour les colonnes
COLUMN_TOOLTIPS = {
    'product_line': 'Ligne de produit',
    'country': 'Pays de destination',
    'value': 'Valeur de la prévision',
    'forecast_type': 'Type de prévision (Budget, Réel, Prévision)',
    'confidence': 'Niveau de confiance de la prévision',
    'actual_fcst': 'Historique des valeurs réelles',
    'full_year_forecast': 'Prévision pour l\'année complète',
    'full_year_budget': 'Budget pour l\'année complète',
    'forecast_vs_budget': 'Écart entre prévision et budget (%)',
    'current_vs_initial': 'Évolution depuis la prévision initiale (%)'
}

# Schémas JSON pour validation
JSON_SCHEMAS = {
    'actual_fcst': {
        "type": "object",
        "patternProperties": {
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}$": {"type": "number"}
        }
    },
    'orderbook': {
        "type": "object",
        "patternProperties": {
            "^[0-9]{4}-[0-9]{2}$": {"type": "number"}
        }
    },
    'budget_dd': {
        "type": "object",
        "patternProperties": {
            "^[0-9]{4}-[0-9]{2}$": {"type": "number"}
        }
    }
}
# Seuils pour les alertes
VARIANCE_THRESHOLD = 0.20  # 20% d'écart

load_dotenv()

# Chemin vers la base de données SQLite
DB_NAME = os.getenv('DB_NAME', 'seqens_forecasts.db')
DB_PATH = os.path.abspath(DB_NAME)


# Affichage pour vérification dans l'app
st.info(f"📁 Base de données utilisée : `{DB_PATH}`")
if not os.path.exists(DB_PATH):
    st.warning("⚠️ Le fichier de base de données n'existe pas à cet emplacement.")
# --- 1× Importer une fois un Excel dans SQLite ---
import pandas as pd
import sqlite3

import pandas as pd
import sqlite3
import json
import hashlib
from datetime import datetime
import os

DB_NAME = os.getenv('DB_NAME', 'seqens_forecasts.db')
DB_PATH = os.path.abspath(DB_NAME)


def import_excel_to_db(excel_file):
    """
    Lit un Excel (en-têtes à la ligne 3), renomme les colonnes d'identité,
    conserve toutes les colonnes mensuelles en format "wide", ajoute id/modifié,
    sérialise correctement les blocs JSON et remplace entièrement la table
    'forecasts' en base SQLite.
    """
    import re, json, sqlite3
    from datetime import datetime

    try:
        # 1) Lecture de l'Excel avec vérification
        try:
            df_raw = pd.read_excel(excel_file, header=2)
            if df_raw.empty:
                st.error("❌ Le fichier Excel est vide.")
                return
        except Exception as e:
            st.error(f"❌ Erreur lors de la lecture du fichier Excel : {e}")
            st.info("Vérifiez que le fichier est au format Excel (.xls ou .xlsx).")
            return

        # Vérification des colonnes requises
        required_columns = ['Clé ship to & article', 'Material Description', 'Product Line']
        missing_columns = [col for col in required_columns if col not in df_raw.columns]
        if missing_columns:
            st.error(f"❌ Colonnes requises manquantes : {', '.join(missing_columns)}")
            st.info("Vérifiez que les en-têtes sont à la ligne 3 du fichier Excel.")
            return

        # 1bis) Nettoyage des noms de colonnes mensuelles
        monthly_pattern = re.compile(r"(?i)(actual\s*&\s*fcst)\s*(\d{4})[/-](\d{1,2})")
        df_raw.columns = [
            re.sub(r"\s+", " ", str(c).replace("\r", " ").replace("\n", " ")).strip()
            for c in df_raw.columns
        ]
        df_raw.columns = [
            f"ACTUAL & FCST {m.group(2)}/{m.group(3).zfill(2)}"
            if (m := monthly_pattern.match(col))
            else col
            for col in df_raw.columns
        ]

        # Retirer toute colonne vide ou NaN
        df_raw = df_raw.loc[:, df_raw.columns.notna()]
        df_raw = df_raw.loc[:, df_raw.columns != ""]

        # 2) Mapping des colonnes d'identité
        column_mapping = {
            'Clé ship to & article': 'ship_to_key',
            'Sales Rep': 'sales_rep',
            'BSO': 'bso',
            'Ship to code': 'ship_to_code',
            'Ship to name': 'ship_to_name',
            'Customer group': 'customer_group',
            'Ship to country': 'country',
            'Material code': 'material_code',
            'Material Description': 'material_description',
            'Prod. hier. level 2': 'prod_hier_level_2',
            'Product Line': 'product_line'
        }
        df = df_raw.rename(columns={old: new for old, new in column_mapping.items() if old in df_raw.columns})

        # Vérifier que les colonnes clés ont été mappées
        key_columns = ['ship_to_key', 'material_description', 'product_line']
        missing_key_columns = [col for col in key_columns if col not in df.columns]
        if missing_key_columns:
            st.error(f"❌ Colonnes clés manquantes après mapping : {', '.join(missing_key_columns)}")
            st.info("Vérifiez que les colonnes d'identité sont présentes dans le fichier Excel.")
            return

        # 3) Ajout des colonnes de traçabilité
        df['id'] = range(1, len(df) + 1)
        df['modified_by'] = 1  # admin par défaut
        df['modified_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 4) Sérialisation des blocs JSON en dicts (avant conversion en texte)
        for prefix in ['ORDERBOOK ', 'BUDGET DD ', 'BACKLOG VARIATION ', 'LAST YEAR ACTUAL ']:
            cols = [c for c in df.columns if c.startswith(prefix)]
            if cols:
                df[prefix.strip().lower().replace(' ', '_')] = df[cols].to_dict(orient='records')

        # 4bis) Calcul des totaux annuels AVANT conversion en JSON
        # → Somme des colonnes ACTUAL & FCST pour full year forecast
        fcst_cols = [c for c in df.columns if c.startswith("ACTUAL & FCST")]
        df['full_year_forecast'] = df[fcst_cols].sum(axis=1) if fcst_cols else 0

        # → Somme des valeurs du dict budget_dd pour full year budget
        df['full_year_budget'] = df['budget_dd'].apply(
            lambda d: sum(d.values()) if isinstance(d, dict) else 0
        )

        # 4ter) Conversion des dict en JSON strings pour SQLite
        for json_col in ['orderbook', 'budget_dd', 'backlog_variation', 'last_year_actual']:
            if json_col in df.columns:
                df[json_col] = df[json_col].apply(lambda d: json.dumps(d, ensure_ascii=False))

        # 5) Écriture en base SQLite (purge + remplacement)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DROP TABLE IF EXISTS forecasts")
        df.to_sql('forecasts', conn, if_exists='replace', index=False)
        conn.close()

        st.success(f"✅ Importation terminée : {len(df)} lignes enregistrées.")

        # 6) Purge des caches et redémarrage
        get_forecasts.clear()      # vide le cache de la fonction get_forecasts
        st.cache_data.clear()      # vide tous les caches @st.cache_data
        st.rerun()                 # relance immédiatement le script Streamlit

    except Exception as e:
        st.error(f"❌ Erreur lors de l'importation : {e}")
        st.info("Vérifiez le format du fichier Excel et la présence des colonnes mensuelles.")





# Styles CSS personnalisés
CUSTOM_CSS = """
<style>
    .stAlert {
        padding: 10px;
        border-radius: 4px;
        margin-bottom: 1rem;
    }
    .tooltip {
        position: relative;
        display: inline-block;
        border-bottom: 1px dotted black;
    }
    .tooltip .tooltiptext {
        visibility: hidden;
        width: 200px;
        background-color: #555;
        color: #fff;
        text-align: center;
        border-radius: 6px;
        padding: 5px;
        position: absolute;
        z-index: 1;
        bottom: 125%;
        left: 50%;
        margin-left: -100px;
        opacity: 0;
        transition: opacity 0.3s;
    }
    .tooltip:hover .tooltiptext {
        visibility: visible;
        opacity: 1;
    }
    .fancy-header {
  background: linear-gradient(
    90deg,
    rgba(30, 120, 220, 1) 0%,   /* Bleu vif de départ */
    rgba(10, 31, 63, 1) 50%,    /* Bleu nuit foncé au milieu */
    rgba(0, 0, 0, 1) 100%       /* Noir à la fin */
  );
  padding: 1rem;
  border-radius: 10px;
  color: white;
  margin-bottom: 2rem;
}



    }
    }
</style>
"""

# Injection du CSS personnalisé
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# UTILITY FUNCTIONS
# -----------------------------------------------------------------------------

import json
import io
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Font

def generate_excel_report(df: pd.DataFrame) -> bytes:
    """
    Crée un fichier Excel avec :
      - un onglet brut 'Données' (colonnes JSON sérialisées en texte)
      - un onglet 'Analyse' avec un résumé par pays et un graphique
    
    Returns:
        bytes : contenu binaire du fichier Excel à transmettre via st.download_button
    """
    # 1) Sérialiser les colonnes JSON en chaînes
    df_export = df.copy()
    json_cols = ["actual_fcst", "last_year_actual", "orderbook", "budget_dd", "backlog_variation"]
    for col in json_cols:
        if col in df_export.columns:
            df_export[col] = df_export[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
            )

    # 2) Création du classeur et de la feuille "Données"
    wb = Workbook()
    ws_data = wb.active
    ws_data.title = "Données"

    # 3) Écriture du DataFrame dans "Données"
    for row in dataframe_to_rows(df_export, index=False, header=True):
        ws_data.append(row)

    # 4) Mise en forme sous forme de tableau
    tab = Table(displayName="TableDonnees", ref=ws_data.dimensions)
    style = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    tab.tableStyleInfo = style
    ws_data.add_table(tab)

    # 5) Création de la feuille "Analyse"
    ws_an = wb.create_sheet("Analyse")
    ws_an["A1"] = "Résumé par Pays"
    ws_an["A1"].font = Font(bold=True)

    # 6) Calcul du résumé par pays et insertion dans "Analyse"
    if "country" in df_export.columns and "value" in df_export.columns:
        summary = df_export.groupby("country", dropna=False)["value"].sum().reset_index()
        # Écriture de l'en-tête + des données
        for row in dataframe_to_rows(summary, index=False, header=True):
            ws_an.append(row)

        # 7) Création du graphique à barres
        chart = BarChart()
        chart.title = "Prévisions par pays"
        chart.x_axis.title = "Pays"
        chart.y_axis.title = "Volume total"

        data_ref = Reference(ws_an,
                             min_col=2,
                             min_row=1,
                             max_row=1 + len(summary))
        cats_ref = Reference(ws_an,
                             min_col=1,
                             min_row=2,
                             max_row=1 + len(summary))
        chart.add_data(data_ref, titles_from_data=True)
        chart.set_categories(cats_ref)
        ws_an.add_chart(chart, "E2")

    # 8) Sauvegarde en mémoire et retour du binaire
    with io.BytesIO() as buffer:
        wb.save(buffer)
        return buffer.getvalue()

def init_packaging_rules_table():
    """Crée la table packaging_rules et ses tables associées si elles n'existent pas"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Table des versions pour le suivi des imports
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS packaging_rules_versions (
            version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            imported_by INTEGER,
            file_name TEXT,
            checksum TEXT,
            record_count INTEGER,
            FOREIGN KEY(imported_by) REFERENCES users(id)
        )
    """)
    
    # Table principale des règles d'emballage
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS packaging_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_code TEXT NOT NULL,
            product_code TEXT NOT NULL,
            packing_size_kg REAL CHECK(packing_size_kg > 0),
            pallet_size_kg REAL CHECK(pallet_size_kg > 0),
            moq_kg REAL CHECK(moq_kg > 0),
            mrq_mt REAL CHECK(mrq_mt > 0),
            last_updated DATE,
            import_version INTEGER,
            UNIQUE(site_code, product_code),
            FOREIGN KEY(import_version) REFERENCES packaging_rules_versions(version_id)
        )
    """)
    
    # Création des index pour optimiser les performances
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_packaging_product_code ON packaging_rules(product_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_packaging_site_code ON packaging_rules(site_code)")
    
    # Table de sauvegarde pour les rollbacks
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS packaging_rules_backup (
            id INTEGER PRIMARY KEY,
            site_code TEXT,
            product_code TEXT,
            packing_size_kg REAL,
            pallet_size_kg REAL,
            moq_kg REAL,
            mrq_mt REAL,
            last_updated DATE,
            import_version INTEGER,
            backup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

def validate_packaging_excel(file):
    """Valide le fichier Excel des règles d'emballage"""
    validation_results = {"success": False, "errors": [], "warnings": []}
    
    try:
        # Vérifier que le fichier peut être ouvert
        try:
            xls = pd.ExcelFile(file)
        except Exception as e:
            validation_results["errors"].append(f"Impossible d'ouvrir le fichier Excel: {str(e)}")
            return validation_results
        
        # Vérifier que l'onglet PackagingRules existe
        if "PackagingRules" not in xls.sheet_names:
            validation_results["errors"].append("L'onglet 'PackagingRules' est absent du fichier Excel")
            return validation_results
        
        # Lire l'onglet
        df = pd.read_excel(file, sheet_name="PackagingRules")
        
        # Vérifier les colonnes requises
        required_columns = ["site_code", "product_code", "packing_size_kg", "pallet_size_kg", 
                           "moq_kg", "mrq_mt", "last_updated"]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_results["errors"].append(f"Colonnes manquantes: {', '.join(missing_columns)}")
            return validation_results
        
        # Vérifier l'ordre des colonnes
        if list(df.columns[:7]) != required_columns:
            validation_results["warnings"].append("L'ordre des colonnes ne correspond pas à l'ordre attendu")
        
        # Vérifier les types de données
        numeric_columns = ["packing_size_kg", "pallet_size_kg", "moq_kg", "mrq_mt"]
        for col in numeric_columns:
            non_numeric = df[~pd.to_numeric(df[col], errors='coerce').notna()]
            if not non_numeric.empty:
                validation_results["errors"].append(f"Valeurs non numériques dans la colonne {col} (lignes: {non_numeric.index.tolist()})")
        
        # Vérifier les valeurs négatives ou nulles
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            invalid_values = df[df[col] <= 0].index.tolist()
            if invalid_values:
                validation_results["errors"].append(f"Valeurs négatives ou nulles dans la colonne {col} (lignes: {invalid_values})")
        
        # Vérifier la cohérence des données
        invalid_relations = df[df['pallet_size_kg'] < df['packing_size_kg']].index.tolist()
        if invalid_relations:
            validation_results["errors"].append(f"La taille de palette est inférieure à la taille d'emballage (lignes: {invalid_relations})")
        
        # Si aucune erreur, marquer comme succès
        if not validation_results["errors"]:
            validation_results["success"] = True
        
        return validation_results
    
    except Exception as e:
        validation_results["errors"].append(f"Erreur lors de la validation: {str(e)}")
        return validation_results

def import_packaging_rules(file, user_id):
    """Importe les règles d'emballage depuis un fichier Excel"""
    import hashlib
    
    # Valider le fichier
    validation = validate_packaging_excel(file)
    if not validation["success"]:
        st.error("❌ Le fichier Excel contient des erreurs:")
        for error in validation["errors"]:
            st.error(f"- {error}")
        return False
    
    # Afficher les avertissements
    for warning in validation["warnings"]:
        st.warning(f"⚠️ {warning}")
    
    try:
        # Calculer le checksum du fichier
        file_content = file.read()
        file.seek(0)  # Réinitialiser le pointeur de fichier
        checksum = hashlib.md5(file_content).hexdigest()
        
        # Lire les données
        df = pd.read_excel(file, sheet_name="PackagingRules")
        
        # Convertir les colonnes numériques
        numeric_columns = ["packing_size_kg", "pallet_size_kg", "moq_kg", "mrq_mt"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convertir la colonne de date
        df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%d')
        
        # Connexion à la base de données
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Créer une sauvegarde de la table actuelle
        cursor.execute("DELETE FROM packaging_rules_backup")
        cursor.execute("INSERT INTO packaging_rules_backup SELECT *, datetime('now') FROM packaging_rules")
        
        # Créer une nouvelle version
        cursor.execute("""
            INSERT INTO packaging_rules_versions (imported_by, file_name, checksum, record_count)
            VALUES (?, ?, ?, ?)
        """, (user_id, file.name, checksum, len(df)))
        
        # Récupérer l'ID de la version
        version_id = cursor.lastrowid
        
        # Commencer une transaction
        conn.execute("BEGIN TRANSACTION")
        
        try:
            # Supprimer les anciennes règles
            cursor.execute("DELETE FROM packaging_rules")
            
            # Insérer les nouvelles règles
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO packaging_rules 
                    (site_code, product_code, packing_size_kg, pallet_size_kg, moq_kg, mrq_mt, last_updated, import_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['site_code'],
                    row['product_code'],
                    row['packing_size_kg'],
                    row['pallet_size_kg'],
                    row['moq_kg'],
                    row['mrq_mt'],
                    row['last_updated'],
                    version_id
                ))
            
            # Valider la transaction
            conn.execute("COMMIT")
            
            # Invalider le cache
            if 'get_packaging_rules' in globals() and hasattr(get_packaging_rules, 'clear'):
                get_packaging_rules.clear()
            if 'get_forecasts' in globals() and hasattr(get_forecasts, 'clear'):
                get_forecasts.clear()
            
            st.success(f"✅ Import réussi: {len(df)} règles d'emballage importées")
            return True
            
        except Exception as e:
            # Annuler la transaction en cas d'erreur
            conn.execute("ROLLBACK")
            st.error(f"❌ Erreur lors de l'import: {str(e)}")
            return False
            
        finally:
            conn.close()
            
    except Exception as e:
        st.error(f"❌ Erreur lors de l'import: {str(e)}")
        return False

@st.cache_data(ttl=3600)
def get_packaging_rules():
    """Récupère toutes les règles d'emballage de la base de données"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM packaging_rules", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Erreur lors de la récupération des règles d'emballage: {str(e)}")
        return pd.DataFrame()


def import_packaging_rules_from_excel(file_path: str) -> dict:
    """
    Lit le fichier Excel transposé des règles d'emballage et fait un upsert dans SQLite.
    Structure attendue (sheet index 0) :
      - ligne 0 : codes produits (colonne A vide ou “Product code” + colonnes B…)
      - ligne 1 : “Packing size (kg)”
      - ligne 2 : “Pallet size (kg)”
      - ligne 3 : “MOQ (kg)”
      - ligne 4 : “MRQ (MT)”
    Retourne : {"inserted": int, "updated": int}
    """
    try:
        import re
        from datetime import datetime

        # 1) Lecture brute du sheet
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        if df.shape[0] < 5 or df.shape[1] < 2:
            st.error("Format incorrect : au moins 5 lignes et 2 colonnes requises")
            return {"inserted": 0, "updated": 0}

        # 2) Extraction des codes produits et des vecteurs
        raw_codes   = df.iloc[0, 1:]
        raw_packing = df.iloc[1, 1:]
        raw_pallet  = df.iloc[2, 1:]
        raw_moq     = df.iloc[3, 1:]
        raw_mrq     = df.iloc[4, 1:]

        # Ne garder que les codes alphanumériques (ex. M001, 123)
        product_codes = []
        for c in raw_codes:
            if pd.isna(c):
                continue
            code = str(c).strip()
            if re.fullmatch(r"[A-Za-z0-9]+", code):
                product_codes.append(code)

        packing_sizes = raw_packing.tolist()
        pallet_sizes  = raw_pallet.tolist()
        moqs          = raw_moq.tolist()
        mrqs          = raw_mrq.tolist()

        # 3) Construction de la liste de dicts
        def extract_number(val):
            if pd.isna(val):
                return None
            if isinstance(val, (int, float)):
                return float(val)
            s = str(val)
            # garder chiffres, points et virgules
            s = re.sub(r"[^\d\.,]", "", s).replace(",", ".")
            m = re.search(r"\d+(\.\d+)?", s)
            return float(m.group()) if m else None

        data = []
        for i, code in enumerate(product_codes):
            ps = extract_number(packing_sizes[i]) or 0
            pls = extract_number(pallet_sizes[i])  or 0
            mq = extract_number(moqs[i])           or 0
            mr = extract_number(mrqs[i])           or 0

            if min(ps, pls, mq, mr) <= 0:
                st.warning(f"Valeurs non valides pour {code} : {ps}, {pls}, {mq}, {mr}")
                continue

            data.append({
                "site_code":       "DEFAULT",
                "product_code":    code,
                "packing_size_kg": ps,
                "pallet_size_kg":  pls,
                "moq_kg":          mq,
                "mrq_mt":          mr,
                "last_updated":    datetime.now().strftime("%Y-%m-%d")
            })

        if not data:
            st.error("Aucune donnée valide extraite")
            return {"inserted": 0, "updated": 0}

        result_df = pd.DataFrame(data)

        # 4) Upsert dans SQLite
        conn   = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        inserted = updated = 0
        conn.execute("BEGIN")
        for _, row in result_df.iterrows():
            cursor.execute(
                "SELECT id FROM packaging_rules WHERE product_code = ? AND site_code = ?",
                (row["product_code"], row["site_code"])
            )
            if cursor.fetchone():
                cursor.execute("""
                    UPDATE packaging_rules
                       SET packing_size_kg = ?, pallet_size_kg = ?, moq_kg = ?, mrq_mt = ?, last_updated = ?
                     WHERE product_code = ? AND site_code = ?
                """, (
                    row["packing_size_kg"],
                    row["pallet_size_kg"],
                    row["moq_kg"],
                    row["mrq_mt"],
                    row["last_updated"],
                    row["product_code"],
                    row["site_code"]
                ))
                updated += 1
            else:
                cursor.execute("""
                    INSERT INTO packaging_rules
                        (site_code, product_code, packing_size_kg, pallet_size_kg, moq_kg, mrq_mt, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["site_code"],
                    row["product_code"],
                    row["packing_size_kg"],
                    row["pallet_size_kg"],
                    row["moq_kg"],
                    row["mrq_mt"],
                    row["last_updated"]
                ))
                inserted += 1

        # Versioning de l’import
        fname = os.path.basename(file_path) if isinstance(file_path, str) else "uploaded"
        cursor.execute("""
            INSERT INTO packaging_rules_versions (imported_by, file_name, record_count)
            VALUES (?, ?, ?)
        """, (1, fname, inserted + updated))

        conn.commit()
        conn.close()

        # Invalidation des caches
        if "get_packaging_rules" in globals() and hasattr(get_packaging_rules, "clear"):
            get_packaging_rules.clear()
        if "get_forecasts" in globals() and hasattr(get_forecasts, "clear"):
            get_forecasts.clear()

        st.success(f"✅ Import terminé : {inserted} insérés, {updated} mis à jour")
        return {"inserted": inserted, "updated": updated}

    except Exception as e:
        st.error(f"❌ Erreur import_packaging_rules_from_excel : {e}")
        return {"inserted": 0, "updated": 0}


def render_packaging_rules_admin():
    """Affiche l'interface d'administration des règles d'emballage"""
    st.markdown("## 📦 Gestion des règles d'emballage")
    
    # Afficher les statistiques actuelles
    try:
        conn = sqlite3.connect(DB_PATH)
        rule_count = pd.read_sql("SELECT COUNT(*) as count FROM packaging_rules", conn).iloc[0]['count']
        last_import = pd.read_sql("""
            SELECT import_date, file_name, record_count 
            FROM packaging_rules_versions 
            ORDER BY import_date DESC LIMIT 1
        """, conn)
        conn.close()
        
        if not last_import.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Règles d'emballage", f"{rule_count}")
            with col2:
                st.metric("Dernier import", f"{last_import.iloc[0]['import_date']}")
            with col3:
                st.metric("Fichier", f"{last_import.iloc[0]['file_name']}")
    except Exception as e:
        st.error(f"❌ Erreur lors de la récupération des statistiques: {str(e)}")
    
    # Interface d'import - Format standard
    with st.expander("📤 Importer un fichier au format standard", expanded=False):
        st.info("Format standard: un onglet 'PackagingRules' avec les colonnes site_code, product_code, etc.")
        uploaded_file = st.file_uploader(
            "Sélectionnez le fichier Excel des règles d'emballage (format standard)",
            type=["xlsx", "xls"],
            key="packaging_rules_file_standard"
        )
        
        if uploaded_file is not None:
            # Afficher un aperçu du fichier
            try:
                preview_df = pd.read_excel(uploaded_file, sheet_name="PackagingRules", nrows=5)
                st.write("Aperçu du fichier:")
                st.dataframe(preview_df)
                
                # Bouton d'import
                if st.button("🚀 Importer les règles d'emballage (format standard)"):
                    uploaded_file.seek(0)  # Réinitialiser le pointeur de fichier
                    success = import_packaging_rules(uploaded_file, st.session_state.user["id"])
                    if success:
                        st.rerun()
            except Exception as e:
                st.error(f"❌ Erreur lors de la lecture du fichier: {str(e)}")
    
    # Interface d'import - Format transposé (nouveau)
    with st.expander("📤 Importer un fichier au format transposé", expanded=True):
        st.info("Format transposé: produits en colonnes, caractéristiques en lignes (Packing, Pallet size, MOQ, MRQ)")
        uploaded_file_transposed = st.file_uploader(
            "Sélectionnez le fichier Excel des règles d'emballage (format transposé)",
            type=["xlsx", "xls"],
            key="packaging_rules_file_transposed"
        )
        
        if uploaded_file_transposed is not None:
            # Afficher un aperçu du fichier
            try:
                preview_df = pd.read_excel(uploaded_file_transposed, sheet_name="Feuil1", header=None, nrows=5)
                st.write("Aperçu du fichier:")
                st.dataframe(preview_df)
                
                # Bouton d'import
                if st.button("🚀 Importer les règles d'emballage (format transposé)"):
                    result = import_packaging_rules_from_excel(uploaded_file_transposed)
                    st.success(f"✅ Import réussi: {result['inserted']} règles ajoutées, {result['updated']} règles mises à jour")
                    st.rerun()
            except Exception as e:
                st.error(f"❌ Erreur lors de la lecture du fichier: {str(e)}")
    
    # Afficher les règles actuelles
    with st.expander("📋 Règles d'emballage actuelles", expanded=False):
        rules_df = get_packaging_rules()
        if not rules_df.empty:
            st.dataframe(rules_df, use_container_width=True)
            
            # Export des règles
            if st.button("📥 Exporter les règles"):
                csv = rules_df.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()
                href = f'<a href="data:file/csv;base64,{b64}" download="regles_emballage.csv">Télécharger le fichier CSV</a>'
                st.markdown(href, unsafe_allow_html=True)
        else:
            st.info("Aucune règle d'emballage n'est définie.")



def generate_collab_report(df_display):
    """
    Génère un rapport Excel complet avec plusieurs onglets et graphiques avancés.
    """
    # Créer un buffer en mémoire pour le fichier Excel
    output = io.BytesIO()
    
    # Nettoyer les données en remplaçant NaN par 0
    df_clean = df_display.fillna(0)
    
    # Identifier les colonnes mensuelles (ACTUAL & FCST)
    fcst_cols = [col for col in df_clean.columns if isinstance(col, str) and col.startswith("ACTUAL & FCST")]
    
    # Créer le fichier Excel
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Formats Excel améliorés
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#1F77B4',
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 12
        })
        
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 16,
            'align': 'center',
            'valign': 'vcenter',
            'font_color': '#1F77B4'
        })
        
        # Formats pour les cellules de données
        num_format = workbook.add_format({
            'num_format': '#,##0',
            'align': 'right',
            'border': 1,
            'bg_color': '#EBF1FA'  # Bleu très clair
        })
        
        text_format = workbook.add_format({
            'align': 'left',
            'border': 1,
            'bg_color': '#F5F5F5'  # Gris très clair
        })
        
        alt_row_format = workbook.add_format({
            'num_format': '#,##0',
            'align': 'right',
            'border': 1,
            'bg_color': '#FFFFFF'  # Blanc
        })
        
        alt_text_format = workbook.add_format({
            'align': 'left',
            'border': 1,
            'bg_color': '#FFFFFF'  # Blanc
        })
        
        # 1. Onglet Détails
        # Écrire directement les données sans utiliser to_excel pour éviter les problèmes d'index
        sheet = workbook.add_worksheet('Détails')
        
        # Ajouter un titre
        sheet.merge_range('A1:E1', 'DÉTAILS DES PRÉVISIONS', title_format)
        
        # Écrire les en-têtes de colonnes
        for col_num, col_name in enumerate(df_clean.columns):
            sheet.write(1, col_num, col_name, header_format)
        
        # Écrire les données
        for row_num, row in enumerate(df_clean.itertuples(index=False)):
            for col_num, value in enumerate(row):
                col_name = df_clean.columns[col_num]
                # Déterminer le format approprié
                if col_name in fcst_cols or col_name.endswith('_kg') or col_name.endswith('_mt') or 'forecast' in col_name.lower() or 'budget' in col_name.lower():
                    fmt = num_format if row_num % 2 == 0 else alt_row_format
                else:
                    fmt = text_format if row_num % 2 == 0 else alt_text_format
                
                sheet.write(row_num + 2, col_num, value, fmt)
        
        # Ajuster la largeur des colonnes
        for i, col in enumerate(df_clean.columns):
            max_len = max(
                df_clean[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2
            sheet.set_column(i, i, min(max_len, 30))
        
        # Activer l'autofiltre
        sheet.autofilter(1, 0, len(df_clean) + 1, len(df_clean.columns) - 1)
        
        # 2. Onglet Résumé par Client avec couleurs
        if 'ship_to_key' in df_clean.columns and 'ship_to_name' in df_clean.columns:
            # Créer le résumé par client
            client_cols = ['ship_to_key', 'ship_to_name']
            if 'country' in df_clean.columns:
                client_cols.append('country')
            
            client_cols += fcst_cols
            if 'full_year_forecast' in df_clean.columns:
                client_cols.append('full_year_forecast')
            
            # Sélectionner uniquement les colonnes qui existent
            client_cols = [col for col in client_cols if col in df_clean.columns]
            client_df = df_clean[client_cols].copy()
            
            # Grouper par client
            group_cols = ['ship_to_key', 'ship_to_name']
            if 'country' in client_cols:
                group_cols.append('country')
            
            client_summary = client_df.groupby(group_cols).sum().reset_index()
            
            # Ajouter une colonne pour le tri
            if 'full_year_forecast' in client_summary.columns:
                sort_col = 'full_year_forecast'
            elif fcst_cols:
                sort_col = fcst_cols[0]
            else:
                sort_col = None
                
            if sort_col:
                client_summary = client_summary.sort_values(by=sort_col, ascending=False)
            
            # Créer la feuille clients manuellement
            sheet = workbook.add_worksheet('Clients')
            
            # Ajouter un titre
            sheet.merge_range('A1:E1', 'RÉSUMÉ PAR CLIENT', title_format)
            
            # Écrire les en-têtes de colonnes
            for col_num, col_name in enumerate(client_summary.columns):
                sheet.write(1, col_num, col_name, header_format)
            
            # Écrire les données
            for row_num, row in enumerate(client_summary.itertuples(index=False)):
                for col_num, value in enumerate(row):
                    col_name = client_summary.columns[col_num]
                    # Déterminer le format approprié
                    if col_name in fcst_cols or col_name.endswith('_kg') or col_name.endswith('_mt') or 'forecast' in col_name.lower() or 'budget' in col_name.lower():
                        fmt = num_format if row_num % 2 == 0 else alt_row_format
                    else:
                        fmt = text_format if row_num % 2 == 0 else alt_text_format
                    
                    sheet.write(row_num + 2, col_num, value, fmt)
            
            # Ajuster les largeurs de colonnes
            for i, col in enumerate(client_summary.columns):
                max_len = max(
                    client_summary[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                sheet.set_column(i, i, min(max_len, 30))
            
            # Activer l'autofiltre
            sheet.autofilter(1, 0, len(client_summary) + 1, len(client_summary.columns) - 1)
        
        # 3. Onglet Résumé par Produit avec couleurs
        if 'product_line' in df_clean.columns:
            # Créer le résumé par produit
            product_cols = ['product_line']
            if 'country' in df_clean.columns:
                product_cols.append('country')
                
            product_cols += fcst_cols
            if 'full_year_forecast' in df_clean.columns:
                product_cols.append('full_year_forecast')
            
            # Sélectionner uniquement les colonnes qui existent
            product_cols = [col for col in product_cols if col in df_clean.columns]
            product_df = df_clean[product_cols].copy()
            
            # Grouper par produit
            group_cols = ['product_line']
            if 'country' in product_cols:
                group_cols.append('country')
                
            product_summary = product_df.groupby(group_cols).sum().reset_index()
            
            # Ajouter une colonne pour le tri
            if 'full_year_forecast' in product_summary.columns:
                sort_col = 'full_year_forecast'
            elif fcst_cols:
                sort_col = fcst_cols[0]
            else:
                sort_col = None
                
            if sort_col:
                product_summary = product_summary.sort_values(by=sort_col, ascending=False)
            
            # Créer la feuille produits manuellement
            sheet = workbook.add_worksheet('Produits')
            
            # Ajouter un titre
            sheet.merge_range('A1:E1', 'RÉSUMÉ PAR PRODUIT', title_format)
            
            # Écrire les en-têtes de colonnes
            for col_num, col_name in enumerate(product_summary.columns):
                sheet.write(1, col_num, col_name, header_format)
            
            # Écrire les données
            for row_num, row in enumerate(product_summary.itertuples(index=False)):
                for col_num, value in enumerate(row):
                    col_name = product_summary.columns[col_num]
                    # Déterminer le format approprié
                    if col_name in fcst_cols or col_name.endswith('_kg') or col_name.endswith('_mt') or 'forecast' in col_name.lower() or 'budget' in col_name.lower():
                        fmt = num_format if row_num % 2 == 0 else alt_row_format
                    else:
                        fmt = text_format if row_num % 2 == 0 else alt_text_format
                    
                    sheet.write(row_num + 2, col_num, value, fmt)
            
            # Ajuster les largeurs de colonnes
            for i, col in enumerate(product_summary.columns):
                max_len = max(
                    product_summary[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                sheet.set_column(i, i, min(max_len, 30))
            
            # Activer l'autofiltre
            sheet.autofilter(1, 0, len(product_summary) + 1, len(product_summary.columns) - 1)
        
        # 4. Onglet Aperçu Mensuel (somme des volumes par mois)
        if fcst_cols:
            monthly_data = []
            for col in fcst_cols:
                match = re.search(r"ACTUAL & FCST (\d{4})/(\d{2})", col)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    date_str = f"{year}/{month}"
                    sort_key = f"{year}{month}"  # Pour le tri
                    
                    # Somme des volumes pour ce mois (prévision)
                    total_volume = df_clean[col].sum()
                    
                    # Extraire les données de budget si disponibles
                    budget_col = f"BUDGET DD {year}/{month} VOL (To)"
                    budget_value = 0
                    if budget_col in df_clean.columns:
                        budget_value = df_clean[budget_col].sum()
                    
                    # Extraire les données réelles depuis ORDERBOOK si disponibles
                    orderbook_col = f"ORDERBOOK {year}/{month}"
                    actual_value = 0
                    if orderbook_col in df_clean.columns:
                        actual_value = df_clean[orderbook_col].sum()
                    # Si la colonne orderbook n'existe pas directement, essayer d'extraire des données JSON
                    elif 'orderbook' in df_clean.columns and isinstance(df_clean['orderbook'].iloc[0], dict):
                        # Si orderbook est un dictionnaire JSON
                        orderbook_key = f"{year}/{month}"
                        actual_value = sum(row.get(orderbook_key, 0) for row in df_clean['orderbook'] if isinstance(row, dict))
                    
                    monthly_data.append({
                        'Mois': date_str,
                        'Volume Total': total_volume,
                        'Budget': budget_value,
                        'Réel': actual_value,
                        'Sort': sort_key
                    })
            
            if monthly_data:
                monthly_df = pd.DataFrame(monthly_data)
                # Trier par date
                monthly_df = monthly_df.sort_values(by='Sort').drop(columns=['Sort'])
                
                # Créer la feuille mensuelle manuellement
                sheet = workbook.add_worksheet('Mensuel')
                
                # Ajouter un titre
                sheet.merge_range('A1:E1', 'APERÇU MENSUEL DES VOLUMES TOTAUX', title_format)
                
                # Écrire les en-têtes de colonnes
                for col_num, col_name in enumerate(monthly_df.columns):
                    sheet.write(1, col_num, col_name, header_format)
                
                # Écrire les données
                for row_num, row in enumerate(monthly_df.itertuples(index=False)):
                    # Mois (colonne 0)
                    sheet.write(row_num + 2, 0, row[0], text_format if row_num % 2 == 0 else alt_text_format)
                    # Autres colonnes numériques
                    for col_idx in range(1, len(monthly_df.columns)):
                        sheet.write(row_num + 2, col_idx, row[col_idx], num_format if row_num % 2 == 0 else alt_row_format)
                
                # Ajuster les largeurs de colonnes
                sheet.set_column(0, 0, 15)  # Mois
                sheet.set_column(1, len(monthly_df.columns) - 1, 20)  # Colonnes numériques
                
                # Activer l'autofiltre
                sheet.autofilter(1, 0, len(monthly_df) + 1, len(monthly_df.columns) - 1)
        
        # 5. Onglet Répartition par Pays
        if 'country' in df_clean.columns:
            # Créer le résumé par pays
            country_cols = ['country'] + fcst_cols
            if 'full_year_forecast' in df_clean.columns:
                country_cols.append('full_year_forecast')
            
            # Sélectionner uniquement les colonnes qui existent
            country_cols = [col for col in country_cols if col in df_clean.columns]
            country_df = df_clean[country_cols].copy()
            
            # Grouper par pays
            country_summary = country_df.groupby(['country']).sum().reset_index()
            
            # Ajouter une colonne pour le tri
            if 'full_year_forecast' in country_summary.columns:
                sort_col = 'full_year_forecast'
            elif fcst_cols:
                sort_col = fcst_cols[0]
            else:
                sort_col = None
                
            if sort_col:
                country_summary = country_summary.sort_values(by=sort_col, ascending=False)
            
            # Créer la feuille pays manuellement
            sheet = workbook.add_worksheet('Pays')
            
            # Ajouter un titre
            sheet.merge_range('A1:E1', 'RÉPARTITION PAR PAYS', title_format)
            
            # Écrire les en-têtes de colonnes
            for col_num, col_name in enumerate(country_summary.columns):
                sheet.write(1, col_num, col_name, header_format)
            
            # Écrire les données
            for row_num, row in enumerate(country_summary.itertuples(index=False)):
                for col_num, value in enumerate(row):
                    col_name = country_summary.columns[col_num]
                    # Déterminer le format approprié
                    if col_name in fcst_cols or col_name.endswith('_kg') or col_name.endswith('_mt') or 'forecast' in col_name.lower() or 'budget' in col_name.lower():
                        fmt = num_format if row_num % 2 == 0 else alt_row_format
                    else:
                        fmt = text_format if row_num % 2 == 0 else alt_text_format
                    
                    sheet.write(row_num + 2, col_num, value, fmt)
            
            # Ajuster les largeurs de colonnes
            for i, col in enumerate(country_summary.columns):
                max_len = max(
                    country_summary[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                sheet.set_column(i, i, min(max_len, 30))
            
            # Activer l'autofiltre
            sheet.autofilter(1, 0, len(country_summary) + 1, len(country_summary.columns) - 1)
        
        # 6. Onglet Dashboard avec graphiques interactifs
        dashboard = workbook.add_worksheet('Dashboard')
        dashboard.merge_range('A1:H1', 'TABLEAU DE BORD DES PRÉVISIONS', title_format)
        
        # Ajouter des instructions pour l'utilisateur
        instructions_format = workbook.add_format({
            'italic': True,
            'font_color': '#666666',
            'font_size': 10
        })
        dashboard.write(2, 0, "Pour personnaliser les graphiques, utilisez les filtres dans les onglets Clients, Produits et Mensuel.", instructions_format)
        
        # 1. Graphique des 10 principaux clients
        if 'Clients' in [sheet.name for sheet in workbook.worksheets()]:
            # Créer le graphique
            client_chart = workbook.add_chart({'type': 'column'})
            
            # Déterminer la colonne de valeur à utiliser
            value_col_idx = 2  # Par défaut, première colonne après les identifiants
            if 'full_year_forecast' in client_summary.columns:
                value_col_idx = client_summary.columns.get_loc('full_year_forecast')
            elif fcst_cols:
                value_col_idx = client_summary.columns.get_loc(fcst_cols[0])
            
            # Limiter aux 10 premiers clients
            max_rows = min(10, len(client_summary))
            
            # Ajouter les données au graphique
            client_chart.add_series({
                'name': 'Volume par client',
                'categories': ['Clients', 2, 1, 2 + max_rows - 1, 1],  # ship_to_name
                'values': ['Clients', 2, value_col_idx, 2 + max_rows - 1, value_col_idx],
                'data_labels': {'value': True},
                'fill': {'color': '#1F77B4'}
            })
            
            # Configurer le graphique
            client_chart.set_title({'name': 'Top 10 Clients par Volume'})
            client_chart.set_x_axis({'name': 'Client'})
            client_chart.set_y_axis({'name': 'Volume'})
            client_chart.set_style(10)
            
            # Insérer le graphique dans le dashboard
            dashboard.insert_chart('A4', client_chart, {'x_scale': 1.5, 'y_scale': 1.5})
        
        # 2. Graphique en camembert des produits
        if 'Produits' in [sheet.name for sheet in workbook.worksheets()]:
            # Créer le graphique
            product_chart = workbook.add_chart({'type': 'pie'})
            
            # Déterminer la colonne de valeur à utiliser
            value_col_idx = 1  # Par défaut, première colonne après product_line
            if 'full_year_forecast' in product_summary.columns:
                value_col_idx = product_summary.columns.get_loc('full_year_forecast')
            elif fcst_cols:
                value_col_idx = product_summary.columns.get_loc(fcst_cols[0])
            
            # Limiter aux 8 premiers produits pour la lisibilité
            max_rows = min(8, len(product_summary))
            
            # Ajouter les données au graphique
            product_chart.add_series({
                'name': 'Répartition par produit',
                'categories': ['Produits', 2, 0, 2 + max_rows - 1, 0],  # product_line
                'values': ['Produits', 2, value_col_idx, 2 + max_rows - 1, value_col_idx],
                'data_labels': {'percentage': True}
            })
            
            # Configurer le graphique
            product_chart.set_title({'name': 'Répartition par Produit'})
            product_chart.set_style(10)
            
            # Insérer le graphique dans le dashboard
            dashboard.insert_chart('A21', product_chart, {'x_scale': 1.5, 'y_scale': 1.5})
        
        # 3. Graphique linéaire de l'évolution mensuelle
        if 'Mensuel' in [sheet.name for sheet in workbook.worksheets()]:
            # Créer le graphique
            monthly_chart = workbook.add_chart({'type': 'line'})
            
            # Ajouter les données au graphique
            monthly_chart.add_series({
                'name': 'Évolution mensuelle des volumes',
                'categories': ['Mensuel', 2, 0, 2 + len(monthly_df) - 1, 0],  # Mois
                'values': ['Mensuel', 2, 1, 2 + len(monthly_df) - 1, 1],  # Volume Total
                'marker': {'type': 'circle', 'size': 8, 'fill': {'color': '#1F77B4'}},
                'line': {'width': 2.5, 'color': '#1F77B4'},
                'data_labels': {'value': True}
            })
            
            # Configurer le graphique
            monthly_chart.set_title({'name': 'Évolution Mensuelle des Volumes'})
            monthly_chart.set_x_axis({'name': 'Mois'})
            monthly_chart.set_y_axis({'name': 'Volume Total'})
            monthly_chart.set_style(10)
            
            # Insérer le graphique dans le dashboard
            dashboard.insert_chart('I4', monthly_chart, {'x_scale': 1.5, 'y_scale': 1.5})
        
        # 4. NOUVEAU: Graphique de comparaison Budget vs Prévision
        if 'Mensuel' in [sheet.name for sheet in workbook.worksheets()] and 'Budget' in monthly_df.columns:
            # Créer le graphique
            budget_chart = workbook.add_chart({'type': 'column'})
            
            # Ajouter les données au graphique - Prévision
            budget_chart.add_series({
                'name': 'Prévision',
                'categories': ['Mensuel', 2, 0, 2 + len(monthly_df) - 1, 0],  # Mois
                'values': ['Mensuel', 2, 1, 2 + len(monthly_df) - 1, 1],  # Volume Total
                'fill': {'color': '#1F77B4'}
            })
            
            # Ajouter les données au graphique - Budget
            budget_chart.add_series({
                'name': 'Budget',
                'categories': ['Mensuel', 2, 0, 2 + len(monthly_df) - 1, 0],  # Mois
                'values': ['Mensuel', 2, 2, 2 + len(monthly_df) - 1, 2],  # Budget
                'fill': {'color': '#FF9900'}
            })
            
            # Configurer le graphique
            budget_chart.set_title({'name': 'Budget vs Prévision par Mois'})
            budget_chart.set_x_axis({'name': 'Mois'})
            budget_chart.set_y_axis({'name': 'Volume'})
            budget_chart.set_style(10)
            
            # Insérer le graphique dans le dashboard
            dashboard.insert_chart('A38', budget_chart, {'x_scale': 1.5, 'y_scale': 1.5})
        
        # 5. NOUVEAU: Graphique de répartition géographique (Carte thermique par pays)
        if 'Pays' in [sheet.name for sheet in workbook.worksheets()]:
            # Créer le graphique
            country_chart = workbook.add_chart({'type': 'bar'})
            
            # Déterminer la colonne de valeur à utiliser
            value_col_idx = 1  # Par défaut, première colonne après country
            if 'full_year_forecast' in country_summary.columns:
                value_col_idx = country_summary.columns.get_loc('full_year_forecast')
            elif fcst_cols:
                value_col_idx = country_summary.columns.get_loc(fcst_cols[0])
            
            # Ajouter les données au graphique
            country_chart.add_series({
                'name': 'Volume par pays',
                'categories': ['Pays', 2, 0, 2 + len(country_summary) - 1, 0],  # country
                'values': ['Pays', 2, value_col_idx, 2 + len(country_summary) - 1, value_col_idx],
                'data_labels': {'value': True},
                'fill': {'color': '#2ECC71'}
            })
            
            # Configurer le graphique
            country_chart.set_title({'name': 'Répartition par Pays'})
            country_chart.set_x_axis({'name': 'Volume'})
            country_chart.set_y_axis({'name': 'Pays'})
            country_chart.set_style(10)
            
            # Insérer le graphique dans le dashboard
            dashboard.insert_chart('I21', country_chart, {'x_scale': 1.5, 'y_scale': 1.5})
        
        # 6. NOUVEAU: Graphique Forecast vs Réalité
        if 'Mensuel' in [sheet.name for sheet in workbook.worksheets()] and 'Réel' in monthly_df.columns:
            # Créer le graphique
            reality_chart = workbook.add_chart({'type': 'line'})
            
            # Ajouter les données au graphique - Prévision
            reality_chart.add_series({
                'name': 'Prévision',
                'categories': ['Mensuel', 2, 0, 2 + len(monthly_df) - 1, 0],  # Mois
                'values': ['Mensuel', 2, 1, 2 + len(monthly_df) - 1, 1],  # Volume Total
                'marker': {'type': 'circle', 'size': 8},
                'line': {'width': 2.5, 'color': '#1F77B4'},
            })
            
            # Ajouter les données au graphique - Réel (ORDERBOOK)
            reality_chart.add_series({
                'name': 'Réel (Orderbook)',
                'categories': ['Mensuel', 2, 0, 2 + len(monthly_df) - 1, 0],  # Mois
                'values': ['Mensuel', 2, 3, 2 + len(monthly_df) - 1, 3],  # Réel
                'marker': {'type': 'diamond', 'size': 8},
                'line': {'width': 2.5, 'color': '#E74C3C', 'dash_type': 'dash'},
            })
            
            # Configurer le graphique
            reality_chart.set_title({'name': 'Prévision vs Commandes Réelles'})
            reality_chart.set_x_axis({'name': 'Mois'})
            reality_chart.set_y_axis({'name': 'Volume'})
            reality_chart.set_style(10)
            
            # Insérer le graphique dans le dashboard
            dashboard.insert_chart('I38', reality_chart, {'x_scale': 1.5, 'y_scale': 1.5})
    
    # Récupérer les bytes du fichier Excel
    output.seek(0)
    return output.getvalue()





def render_client_management_tab():
    """
    Affiche l'onglet de gestion des clients pour les vendeurs et les administrateurs
    """
    st.markdown("## 👥 Gestion des clients")
    
    user_id = st.session_state.user["id"]
    user_role = st.session_state.user["role"]
    
    # Récupérer les clients assignés au vendeur connecté
    conn = sqlite3.connect(DB_PATH)
    
    # Récupérer tous les clients disponibles
    all_clients_df = pd.read_sql("""
        SELECT DISTINCT ship_to_key, ship_to_name, ship_to_code, country
        FROM forecasts
        WHERE ship_to_key IS NOT NULL
        ORDER BY ship_to_key
    """, conn)
    
    # Récupérer les clients déjà assignés
    assigned_clients_df = pd.read_sql("""
        SELECT ca.ship_to_key, ca.ship_to_name, ca.ship_to_code, ca.ship_to_country, u.full_name as assigned_to
        FROM client_assignments ca
        JOIN users u ON ca.sales_rep_id = u.id
        WHERE ca.sales_rep_id = ?
    """, conn, params=[user_id])
    
    conn.close()
    
    # Afficher les clients assignés
    st.subheader("Mes clients assignés")
    if not assigned_clients_df.empty:
        st.dataframe(assigned_clients_df)
    else:
        st.info("Aucun client ne vous est assigné pour le moment.")
    
    # Formulaire pour ajouter un client
    with st.expander("➕ Ajouter un client", expanded=False):
        with st.form("add_client_form"):
            # Filtrer les clients qui ne sont pas déjà assignés
            assigned_keys = assigned_clients_df['ship_to_key'].tolist() if not assigned_clients_df.empty else []
            available_clients = all_clients_df[~all_clients_df['ship_to_key'].isin(assigned_keys)]
            
            if available_clients.empty:
                st.warning("Tous les clients disponibles vous sont déjà assignés.")
                submit_disabled = True
            else:
                submit_disabled = False
                
                # Créer un dictionnaire pour l'affichage
                client_display = {}
                for _, row in available_clients.iterrows():
                    key = row['ship_to_key']
                    name = row['ship_to_name'] if pd.notna(row['ship_to_name']) else "Sans nom"
                    client_display[key] = f"{key} - {name}"
                
                # Sélection du client à ajouter
                selected_client = st.selectbox(
                    "Sélectionner un client à ajouter",
                    options=available_clients['ship_to_key'].tolist(),
                    format_func=lambda x: client_display.get(x, x)
                )
                
                # Récupérer les informations du client sélectionné
                client_info = available_clients[available_clients['ship_to_key'] == selected_client].iloc[0]
            
            submit = st.form_submit_button("Ajouter ce client", disabled=submit_disabled)
            
            if submit and not submit_disabled:
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cursor = conn.cursor()
                    
                    # Insérer le client dans la table des assignations
                    cursor.execute("""
                        INSERT INTO client_assignments 
                        (sales_rep_id, ship_to_key, ship_to_code, ship_to_name, ship_to_country)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        user_id,
                        client_info['ship_to_key'],
                        client_info['ship_to_code'] if pd.notna(client_info['ship_to_code']) else "",
                        client_info['ship_to_name'] if pd.notna(client_info['ship_to_name']) else "",
                        client_info['country'] if pd.notna(client_info['country']) else ""
                    ))
                    
                    conn.commit()
                    conn.close()
                    
                    st.success(f"✅ Client {client_info['ship_to_key']} ajouté avec succès!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur lors de l'ajout du client : {e}")
    

def reset_database():
    """
    Réinitialise complètement la table des prévisions dans la base de données.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Supprimer la table forecasts
        conn.execute("DROP TABLE IF EXISTS forecasts")
        # Recréer une table vide avec la structure minimale
        conn.execute("""
            CREATE TABLE forecasts (
                id INTEGER PRIMARY KEY,
                ship_to_key TEXT,
                material_description TEXT,
                product_line TEXT,
                modified_by INTEGER,
                modified_at DATETIME
            )
        """)
        conn.commit()
        conn.close()
        
        # Vider le cache
        if hasattr(get_forecasts, 'clear'):
            get_forecasts.clear()
        st.cache_data.clear()
        
        return True
    except Exception as e:
        st.error(f"Erreur lors de la réinitialisation de la base de données : {str(e)}")
        return False



def render_forecast_analysis_tab():
    """

    Affiche l'onglet d'analyse avancée avec :
     - 📊 Visualisations standard (avec export Excel enrichi)
     - 🔮 Prévisions automatiques (Prophet)
    """
    import re 

    tab1, tab2 = st.tabs(["📊 Visualisations standard", "🔮 Prévisions automatiques"])

    # --- Onglet 1 : Visualisations standard ---
    with tab1:
        df = get_forecasts()
        if df.empty:
            st.info("Aucune donnée de prévision disponible.")
        else:
            # Vérifier si la colonne 'value' existe, sinon utiliser 'full_year_forecast'
            value_column = 'full_year_forecast' if 'full_year_forecast' in df.columns else None
            
            if not value_column:
                # Chercher une colonne ACTUAL & FCST
                fcst_cols = [col for col in df.columns if isinstance(col, str) and "ACTUAL & FCST" in col]
                if fcst_cols:
                    value_column = fcst_cols[0]
                else:
                    st.error("Aucune colonne de valeur trouvée dans les données.")
                    return
            
            with st.expander("📈 Visualisations", expanded=True):
                col1, col2 = st.columns(2)

                with col1:
                    country_data = df.groupby('country')[value_column].sum().reset_index()
                    fig1 = px.bar(country_data, x='country', y=value_column,
                                  title="Prévisions par pays",
                                  labels={'country': 'Pays', value_column: 'Volume total'})
                    st.plotly_chart(fig1, use_container_width=True)

                with col2:
                    if 'client_type' in df.columns:
                        client_data = df.groupby('client_type')[value_column].sum().reset_index()
                        fig2 = px.pie(client_data, names='client_type', values=value_column,
                                     title='Répartition par type de client')
                        st.plotly_chart(fig2, use_container_width=True)

            with st.expander("📅 Analyse temporelle", expanded=True):
                # Créer une colonne date si elle n'existe pas
                if 'date' not in df.columns:
                    # Utiliser la première colonne ACTUAL & FCST pour extraire la date
                    fcst_cols = [col for col in df.columns if isinstance(col, str) and "ACTUAL & FCST" in col]
                    if fcst_cols:
                        df['date'] = pd.to_datetime(fcst_cols[0].replace("ACTUAL & FCST ", "") + "-01")
                    else:
                        df['date'] = pd.to_datetime('today')
                else:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                
                time_data = df.groupby('date')[value_column].sum().reset_index()
                fig3 = px.line(time_data, x='date', y=value_column,
                               title="Évolution temporelle des prévisions",
                               labels={'date': 'Date', value_column: 'Volume total'})
                st.plotly_chart(fig3, use_container_width=True)

                if st.checkbox("📊 Afficher les statistiques descriptives"):
                    stats_df = (
                        df.groupby('product_line')[value_column]
                        .agg(['count', 'mean', 'std', 'min', 'max'])
                        .round(2)
                    )
                    st.dataframe(stats_df)

            if st.button("📥 Exporter vers Excel", use_container_width=True):
                xls_bytes = generate_excel_report(df)
                st.download_button(
                    label="📤 Télécharger le rapport Excel",
                    data=xls_bytes,
                    file_name="rapport_previsions.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    # --- Onglet 2 : Prévisions automatiques ---
    with tab2:
        st.markdown("## 🔮 Prévisions automatiques")
        df = get_forecasts()
        if df.empty:
            st.info("Aucune donnée disponible pour générer des prévisions.")
            return

        # Trouver les colonnes ACTUAL & FCST pour les prévisions
        fcst_cols = [col for col in df.columns if isinstance(col, str) and "ACTUAL & FCST" in col]
        if not fcst_cols:
            st.error("Aucune colonne 'ACTUAL & FCST' trouvée pour les prévisions.")
            return
            
        # Créer une série temporelle à partir des colonnes ACTUAL & FCST
        data_series = []
        for idx, row in df.iterrows():
            for col in fcst_cols:
                if pd.notna(row[col]):
                    # Extraire la date du nom de colonne
                    date_match = re.search(r"ACTUAL & FCST (\d{4})/(\d{2})", col)
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2)
                        data_series.append({
                            'date': pd.to_datetime(f"{year}-{month}-01"),
                            'value': float(row[col]),
                            'product_line': row['product_line'],
                            'country': row['country']
                        })
        
        # Créer un DataFrame avec les séries temporelles
        time_series_df = pd.DataFrame(data_series)
        if time_series_df.empty:
            st.error("Impossible de créer une série temporelle à partir des données.")
            return

        st.markdown("### Sélectionnez les paramètres de prévision")
        col1, col2 = st.columns(2)

        products = sorted([p for p in time_series_df['product_line'].dropna().unique()])
        countries = sorted([c for c in time_series_df['country'].dropna().unique()])

        with col1:
            if not products:
                st.error("Aucun produit valide trouvé.")
                return
            selected_product = st.selectbox("Produit", products)

        with col2:
            if not countries:
                st.error("Aucun pays valide trouvé.")
                return
            selected_country = st.selectbox("Pays", countries)

        horizon = st.slider("Horizon de prévision (mois)", 1, 12, 6)

        if st.button("🔮 Générer les prévisions", use_container_width=True):
            filtered = time_series_df[
                (time_series_df['product_line'] == selected_product) &
                (time_series_df['country'] == selected_country)
            ].sort_values('date')

            if len(filtered) < 3:
                st.warning("⚠️ Pas assez de données pour une prévision fiable.")
                return

            data_prophet = filtered[['date', 'value']].dropna()
            data_prophet['value'] = pd.to_numeric(data_prophet['value'], errors='coerce')
            data_prophet = data_prophet.dropna()

            if len(data_prophet) < 3:
                st.warning("⚠️ Données numériques insuffisantes.")
                return

            prophet_df = data_prophet.rename(columns={'date': 'ds', 'value': 'y'})
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=False,
                daily_seasonality=False,
                seasonality_mode='multiplicative',
                interval_width=0.8
            )
            model.fit(prophet_df)
            future = model.make_future_dataframe(periods=horizon, freq="M")
            forecast = model.predict(future)

            res = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
            res[['yhat', 'yhat_lower', 'yhat_upper']] = res[['yhat', 'yhat_lower', 'yhat_upper']].apply(
                pd.to_numeric, errors='coerce'
            ).fillna(0)
            res = res.rename(columns={
                'ds': 'date', 'yhat': 'forecast',
                'yhat_lower': 'lower_bound', 'yhat_upper': 'upper_bound'
            })

            st.success("✅ Prévisions générées !")

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=filtered['date'], y=filtered['value'],
                                     mode='lines+markers', name='Historique'))
            fig.add_trace(go.Scatter(x=res['date'], y=res['forecast'],
                                     mode='lines+markers', name='Prévision',
                                     line=dict(dash='dash')))
            x_vals = list(res['date']) + list(res['date'][::-1])
            y_vals = list(res['upper_bound']) + list(res['lower_bound'][::-1])
            fig.add_trace(go.Scatter(
                x=x_vals, y=y_vals,
                fill='toself', fillcolor='rgba(200,200,200,0.2)',
                line=dict(color='rgba(255,255,255,0)'), name='Intervalle confiance'
            ))
            fig.update_layout(
                title=f"{selected_product} — {selected_country}",
                xaxis_title="Date", yaxis_title="Volume"
            )
            st.plotly_chart(fig, use_container_width=True)

            display = res[['date', 'forecast', 'lower_bound', 'upper_bound']].copy()
            display['date'] = display['date'].dt.strftime('%Y-%m-%d')
            display.columns = ['Date', 'Prévision', 'Borne inférieure', 'Borne supérieure']
            st.dataframe(display, use_container_width=True)

            csv2 = display.to_csv(index=False).encode()
            b642 = base64.b64encode(csv2).decode()
            href2 = (
                f'<a href="data:file/csv;base64,{b642}" '
                f'download="previsions_{selected_product}_{selected_country}.csv">Télécharger</a>'
            )
            st.markdown(href2, unsafe_allow_html=True)




def ensure_internal_comment_column():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # Récupère la liste des colonnes existantes
        cursor.execute("PRAGMA table_info(forecasts)")
        columns = [col[1] for col in cursor.fetchall()]
        # Si la colonne n'existe pas, on l'ajoute
        if "internal_comment" not in columns:
            cursor.execute("ALTER TABLE forecasts ADD COLUMN internal_comment TEXT")
            conn.commit()
            print("✅ Colonne 'internal_comment' ajoutée avec succès.")
        conn.close()
    except Exception as e:
        print(f"❌ Erreur lors de l'ajout de la colonne 'internal_comment' : {e}")


def render_forecast_tab():
    """
    Affiche l'onglet des prévisions avec le tableau collaboratif et les graphiques
    """
    st.markdown("## 📊 Prévisions collaboratives")
    
    # Affichage du tableau collaboratif
    updated_df, selected_rows = render_collaborative_table()
    
    if not updated_df.empty:
        # Affichage des graphiques si des données sont disponibles
        with st.expander("📈 Visualisations", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Graphique des prévisions par pays
                country_data = updated_df.groupby('country')['value'].sum().reset_index()
                fig1 = px.bar(
                    country_data,
                    x='country',
                    y='value',
                    title='Prévisions par pays',
                    labels={'value': 'Volume total', 'country': 'Pays'}
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                # Graphique des prévisions par type de client
                if 'client_type' in updated_df.columns:
                    client_data = updated_df.groupby('client_type')['value'].sum().reset_index()
                    fig2 = px.pie(
                        client_data,
                        values='value',
                        names='client_type',
                        title='Répartition par type de client'
                    )
                    st.plotly_chart(fig2, use_container_width=True)
        
        # Analyse temporelle
        with st.expander("📅 Analyse temporelle", expanded=True):
            # Conversion de la colonne date en datetime
            updated_df['date'] = pd.to_datetime(updated_df['date'])
            
            # Graphique d'évolution temporelle
            time_data = updated_df.groupby('date')['value'].sum().reset_index()
            fig3 = px.line(
                time_data,
                x='date',
                y='value',
                title='Évolution temporelle des prévisions',
                labels={'value': 'Volume total', 'date': 'Date'}
            )
            st.plotly_chart(fig3, use_container_width=True)
            
            # Statistiques descriptives
            if st.checkbox("📊 Afficher les statistiques descriptives"):
                st.write("### Statistiques descriptives")
                stats_df = updated_df.groupby('product_line')['value'].agg([
                    'count', 'mean', 'std', 'min', 'max'
                ]).round(2)
                st.dataframe(stats_df)
        
                # Export des données
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📄 Exporter en CSV", use_container_width=True):
                csv = updated_df.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()
                href = f'<a href="data:file/csv;base64,{b64}" download="previsions.csv">Télécharger le fichier CSV</a>'
                st.markdown(href, unsafe_allow_html=True)
        
        with col2:
            if st.button("📊 Exporter en Excel", use_container_width=True):
                xlsx_bytes = generate_collab_report(updated_df)
                st.download_button(
                    "📥 Télécharger le rapport Excel complet",
                    xlsx_bytes,
                    file_name=f"rapport_collaboratif_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
    else:
        st.info("Aucune donnée de prévision disponible.")





def get_user_by_username(username):
    """
    Récupère les informations d'un utilisateur par son nom d'utilisateur
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, username, password_hash, full_name, role FROM users WHERE username = ?",
            (username,)
        )
        user = cursor.fetchone()
        if user:
            return {
                "id": user[0],
                "username": user[1],
                "password_hash": user[2],
                "full_name": user[3],
                "role": user[4]
            }
        return None
    except Exception as e:
        st.error(f"Erreur lors de la récupération de l'utilisateur : {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def verify_password(password, password_hash):
    """
    Vérifie si le mot de passe correspond au hash stocké
    """
    try:
        # Conversion du mot de passe en hash SHA-256
        input_hash = hashlib.sha256(password.encode()).hexdigest()
        # Comparaison avec le hash stocké
        return input_hash == password_hash
    except Exception as e:
        st.error(f"Erreur lors de la vérification du mot de passe : {str(e)}")
        return False


import uuid
from datetime import datetime, timedelta

def login_screen_secure():
    st.markdown("### 🔐 Connexion sécurisée")

    # Vérifie si un token actif est présent
    if "user" not in st.session_state and "remember_token" in st.session_state:
        token = st.session_state["remember_token"]
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM users WHERE remember_token = ? AND token_expiry > datetime('now')
        """, (token,))
        row = cur.fetchone()
        conn.close()
        if row:
            st.session_state.user = {
                "id": row[0],
                "username": row[1],
                "password_hash": row[2],
                "full_name": row[3],
                "role": row[4]
            }
            st.success("🔓 Connexion automatique réussie")
            return

    with st.form("secure_login_form"):
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        remember = st.checkbox("Se souvenir de moi")
        submit = st.form_submit_button("Connexion")

        if submit:
            user_data = get_user_by_username(username)
            if user_data and verify_password(password, user_data["password_hash"]):
                st.session_state.user = user_data

                # Génère et stocke un token si demandé
                if remember:
                    token = str(uuid.uuid4())
                    expiry = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S')
                    st.session_state["remember_token"] = token

                    conn = sqlite3.connect(DB_PATH)
                    conn.execute("""
                        UPDATE users SET remember_token = ?, token_expiry = ?
                        WHERE id = ?
                    """, (token, expiry, user_data["id"]))
                    conn.commit()
                    conn.close()
                else:
                    # Supprime token si présent
                    st.session_state.pop("remember_token", None)
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute("""
                        UPDATE users SET remember_token = NULL, token_expiry = NULL
                        WHERE id = ?
                    """, (user_data["id"],))
                    conn.commit()
                    conn.close()

                st.success("✅ Connexion réussie")
                st.rerun()
            else:
                st.error("❌ Identifiants incorrects")


def alert_style(value, threshold=0.30):
    try:
        return 'background-color: #f8d7da' if abs(value) > threshold else ''
    except:
        return ''

def safe_datetime_format(date_value, format='%Y-%m-%d %H:%M'):
    """
    Formate une date de manière sécurisée en gérant les cas NaT
    """
    try:
        if pd.isna(date_value) or pd.isnull(date_value):
            return None
        return pd.to_datetime(date_value).strftime(format)
    except:
        return None
    
def upgrade_forecast_table_if_needed():
    """
    Ajoute les colonnes manquantes à la table forecasts si elles n'existent pas encore.
    À appeler au démarrage de l'app, juste après init_database().
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    columns_to_add = {
        "firm_order": "INTEGER DEFAULT 0",
        "client_type": "TEXT",
        "module": "TEXT",
        "note": "TEXT",
        "delivery_week": "INTEGER"
    }

    for col_name, col_type in columns_to_add.items():
        try:
            cur.execute(f"ALTER TABLE forecasts ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                st.error(f"Erreur ajout colonne {col_name}: {e}")

    conn.commit()
    conn.close()


@st.cache_data(ttl=3600)
def generate_prophet_forecast(df, product_line=None, country=None, horizon_months=6, include_history=True):
    """
    Génère une prévision automatique via Prophet pour un produit/pays donné.
    
    Parameters:
    -----------
    df : DataFrame
        Données historiques complètes
    product_line : str
        Ligne de produit à filtrer (optionnel)
    country : str
        Pays à filtrer (optionnel)
    horizon_months : int
        Nombre de mois à prévoir
    include_history : bool
        Inclure les données historiques dans le résultat
        
    Returns:
    --------
    DataFrame avec les prévisions et les données historiques
    """
    try:
        # Filtrer les données si nécessaire
        filtered_df = df.copy()
        if product_line and country:
            filtered_df = filtered_df[
                (filtered_df['product_line'] == product_line) & 
                (filtered_df['country'] == country)
            ]
        elif product_line:
            filtered_df = filtered_df[filtered_df['product_line'] == product_line]
        elif country:
            filtered_df = filtered_df[filtered_df['country'] == country]
        
        # Agréger par date si nécessaire
        if 'date' in filtered_df.columns:
            agg_df = filtered_df.groupby('date')['value'].sum().reset_index()
        else:
            agg_df = filtered_df
        
        # Vérifier qu'il y a assez de données
        if len(agg_df) < 3:
            st.warning("⚠️ Pas assez de données pour générer une prévision fiable.")
            return None
        
        # S'assurer que la colonne value est numérique
        agg_df['value'] = pd.to_numeric(agg_df['value'], errors='coerce')
        agg_df = agg_df.dropna(subset=['value'])
        
        if len(agg_df) < 3:
            st.warning("⚠️ Pas assez de données numériques valides pour générer une prévision.")
            return None
        
        # Préparer les données pour Prophet
        prophet_df = agg_df.rename(columns={"date": "ds", "value": "y"})
        prophet_df = prophet_df[["ds", "y"]].dropna()
        
        # Créer et entraîner le modèle
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            seasonality_mode='multiplicative',
            interval_width=0.8  # 80% d'intervalle de confiance
        )
        model.fit(prophet_df)
        
        # Générer les prévisions
        future = model.make_future_dataframe(periods=horizon_months, freq="M")
        forecast = model.predict(future)
        
        # Préparer le résultat et s'assurer que les colonnes sont numériques
        result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        
        # Convertir explicitement en numérique
        result["yhat"] = pd.to_numeric(result["yhat"], errors='coerce')
        result["yhat_lower"] = pd.to_numeric(result["yhat_lower"], errors='coerce')
        result["yhat_upper"] = pd.to_numeric(result["yhat_upper"], errors='coerce')
        
        # Renommer les colonnes
        result = result.rename(columns={
            "ds": "date", 
            "yhat": "forecast", 
            "yhat_lower": "lower_bound", 
            "yhat_upper": "upper_bound"
        })
        
        # Ajouter une colonne pour distinguer historique et prévision
        last_date = prophet_df["ds"].max()
        result["type"] = "forecast"
        result.loc[result["date"] <= last_date, "type"] = "historical"
        
        # Ajouter les valeurs réelles pour l'historique
        if include_history:
            historical_values = prophet_df.set_index("ds")["y"]
            result.loc[result["date"] <= last_date, "actual"] = result.loc[result["date"] <= last_date, "date"].map(historical_values)
            # S'assurer que la colonne actual est numérique
            if "actual" in result.columns:
                result["actual"] = pd.to_numeric(result["actual"], errors='coerce')
        
        # Vérifier qu'il n'y a pas de valeurs NaN dans les colonnes numériques
        result = result.fillna({
            "forecast": 0,
            "lower_bound": 0,
            "upper_bound": 0,
            "actual": 0
        })
        
        return result

    except Exception as e:
        st.error(f"❌ Erreur modèle Prophet : {e}")
        import traceback
        st.error(traceback.format_exc())
        return None




def get_country_column_name(conn):
    """Retourne le nom correct à utiliser pour la colonne pays dans la table sales."""
    try:
        df_check = pd.read_sql("PRAGMA table_info(sales);", conn)
        available_columns = df_check['name'].tolist()

        if "country" in available_columns:
            return "country"
        elif "country_alt" in available_columns:
            return "country_alt"
        else:
            return None
    except:
        return None


def validate_json_data(data, schema_name):
    """
    Valide les données JSON selon le schéma défini
    """
    try:
        if isinstance(data, str):
            data = json.loads(data)
        jsonschema.validate(instance=data, schema=JSON_SCHEMAS[schema_name])
        return True, None
    except jsonschema.exceptions.ValidationError as e:
        return False, str(e)
    except json.JSONDecodeError as e:
        return False, "Format JSON invalide"

VARIANCE_THRESHOLD = 0.3  # Exemple : 30% d’écart

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
        return False  # aucun écart s’il n’y a rien des deux côtés

    baseline = (abs(v1) + abs(v2)) / 2
    if baseline == 0:
        return False  # éviter toute division par 0

    relative_diff = abs(v1 - v2) / baseline
    return relative_diff > VARIANCE_THRESHOLD

def create_tooltip(text, tooltip_text):
    """
    Crée un élément HTML avec tooltip
    """
    return f"""
        <div class="tooltip">
            {text}
            <span class="tooltiptext">{tooltip_text}</span>
        </div>
    """

def format_number(value):
    """
    Formate les nombres pour l'affichage
    """
    if isinstance(value, (int, float)):
        if value >= 1000000:
            return f"{value/1000000:.1f}M"
        elif value >= 1000:
            return f"{value/1000:.1f}k"
        else:
            return f"{value:.1f}"
    return str(value)

def add_variance_indicators(df):
    """
    Ajoute des indicateurs visuels pour les écarts significatifs entre :
    - Prévision vs Budget
    - Prévision vs Moyenne historique
    """
    def safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    df['variance_indicator'] = ''

    for idx, row in df.iterrows():
        forecast_val = safe_float(row.get('value'))
        budget_val   = safe_float(row.get('full_year_budget'))

        # ✅ Vérif écart prévision vs budget
        if forecast_val is not None and budget_val is not None:
            if check_variance(forecast_val, budget_val):
                df.at[idx, 'variance_indicator'] += '⚠️ Écart budget significatif\n'

        # ✅ Vérif écart prévision vs historique
        hist_raw = row.get('actual_fcst')
        if isinstance(hist_raw, dict) and hist_raw:
            hist_values = [safe_float(v) for v in hist_raw.values()]
            hist_values = [v for v in hist_values if v is not None]  # garde que les floats valides
            if hist_values:
                avg_hist = sum(hist_values) / len(hist_values)
                if forecast_val is not None and check_variance(forecast_val, avg_hist):
                    df.at[idx, 'variance_indicator'] += '📊 Écart historique significatif\n'

    return df


@st.cache_data(ttl=600)
def get_monthly_sales_summary():
    try:
        conn = sqlite3.connect(DB_PATH)

        # Vérifie les colonnes présentes dans la table 'sales'
        df_check = pd.read_sql("PRAGMA table_info(sales);", conn)
        available_columns = df_check['name'].tolist()

        # Utiliser country_alt si country n'existe pas
        if "country" in available_columns:
            country_col = "country"
        elif "country_alt" in available_columns:
            country_col = "country_alt"
        else:
            country_col = None

        # Construire la requête SQL dynamiquement
        if country_col:
            query = f"""
                SELECT 
                    strftime('%Y-%m', order_date) as ym,
                    SUM(invoiced_flag) as total,
                    {country_col}
                FROM sales 
                GROUP BY ym, {country_col}
                ORDER BY ym
            """
        else:
            query = """
                SELECT 
                    strftime('%Y-%m', order_date) as ym,
                    SUM(invoiced_flag) as total
                FROM sales 
                GROUP BY ym
                ORDER BY ym
            """

        sales_summary = pd.read_sql(query, conn)
        return sales_summary

    except Exception as e:
        st.error(f"Erreur lors du chargement des données : {str(e)}")
        return pd.DataFrame()

    finally:
        if 'conn' in locals():
            conn.close()







# -----------------------------------------------------------------------------
# DATABASE FUNCTIONS
# -----------------------------------------------------------------------------
def init_database():
    """
    Initialise la base de données avec toutes les tables nécessaires.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ✅ Table des utilisateurs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            remember_token TEXT,
            token_expiry DATETIME,
            created_at DATETIME DEFAULT (datetime('now', 'localtime'))
        )
    """)

    # ✅ Table des assignations de clients aux vendeurs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS client_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sales_rep_id INTEGER NOT NULL,
            ship_to_key TEXT NOT NULL,
            ship_to_code TEXT NOT NULL,
            ship_to_name TEXT NOT NULL,
            ship_to_country TEXT,
            active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT (datetime('now', 'localtime')),
            modified_at DATETIME DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY(sales_rep_id) REFERENCES users(id),
            UNIQUE(sales_rep_id, ship_to_key)
        )
    """)

    # ✅ Table des prévisions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            id INTEGER PRIMARY KEY,
            product_line TEXT,
            country TEXT,
            date TEXT,
            value REAL,
            forecast_type TEXT,
            confidence TEXT,
            actual_fcst TEXT,
            ship_to_key TEXT,
            sales_rep TEXT,
            bso TEXT,
            ship_to_code TEXT,
            ship_to_name TEXT,
            customer_group TEXT,
            ship_to_country TEXT,
            material_code TEXT,
            material_description TEXT,
            prod_hier_level_2 TEXT,
            last_year_actual JSON,
            orderbook JSON,
            budget_dd JSON,
            backlog_variation JSON,
            full_year_forecast REAL,
            full_year_budget REAL,
            forecast_vs_budget REAL,
            current_vs_initial REAL,
            data_source TEXT,
            firm_order INTEGER DEFAULT 0,
            client_type TEXT,
            module TEXT,
            delivery_week INTEGER,
            note TEXT,
            modified_by INTEGER,
            modified_at DATETIME DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY(modified_by) REFERENCES users(id)
        )
    """)

    # ✅ Table des ventes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            period TEXT,
            week INTEGER,
            sales_document TEXT,
            item_order TEXT,
            po_number TEXT,
            delivery_no TEXT,
            transport_no TEXT,
            packaging_status TEXT,
            billing_document TEXT,
            sales_organisation TEXT,
            plant TEXT,
            created_by TEXT,
            ship_to_code TEXT,
            ship_to TEXT,
            zone_main TEXT,
            zone_alt TEXT,
            country TEXT,
            country_alt TEXT,
            city TEXT,
            sold_to_code TEXT,
            sold_to TEXT,
            bu_cust_group TEXT,
            customer_group TEXT,
            cust_rep TEXT,
            sales_adm TEXT,
            article_number TEXT,
            article TEXT,
            subcategory TEXT,
            product_line_code TEXT,
            incoterms TEXT,
            incoterms_part2 TEXT,
            currency TEXT,
            freight_type TEXT,
            freight_type_desc TEXT,
            order_date DATE,
            delivery_date DATE,
            invoiced REAL,
            g_i_req_date DATE
        )
    """)

    # ✅ Table des commentaires sur les prévisions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast_comments (
            id INTEGER PRIMARY KEY,
            forecast_id INTEGER,
            comment_text TEXT,
            user_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(forecast_id) REFERENCES forecasts(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    # ✅ Table des règles d'emballage
    cur.execute("""
        CREATE TABLE IF NOT EXISTS packaging_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_code TEXT NOT NULL,
            product_code TEXT NOT NULL,
            packing_size_kg REAL CHECK(packing_size_kg > 0),
            pallet_size_kg REAL CHECK(pallet_size_kg > 0),
            moq_kg REAL CHECK(moq_kg > 0),
            mrq_mt REAL CHECK(mrq_mt > 0),
            last_updated DATE,
            import_version INTEGER,
            UNIQUE(site_code, product_code)
        )
    """)

    # ✅ Table des versions des règles d'emballage
    cur.execute("""
        CREATE TABLE IF NOT EXISTS packaging_rules_versions (
            version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            imported_by INTEGER,
            file_name TEXT,
            checksum TEXT,
            record_count INTEGER,
            FOREIGN KEY(imported_by) REFERENCES users(id)
        )
    """)

    # ✅ Recréation de l'historique
    cur.execute("DROP TABLE IF EXISTS forecast_history")
    cur.execute("""
        CREATE TABLE forecast_history (
            id INTEGER PRIMARY KEY,
            forecast_id INTEGER,
            field_name TEXT,
            old_value TEXT,
            new_value TEXT,
            modified_by INTEGER,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(forecast_id) REFERENCES forecasts(id),
            FOREIGN KEY(modified_by) REFERENCES users(id)
        )
    """)

    # Assurer que des clients sont assignés pour les tests
    ensure_client_assignments()

    # ✅ Admin par défaut si aucun utilisateur admin
    cur.execute("SELECT COUNT(*) FROM users WHERE role='admin'")
    if cur.fetchone()[0] == 0:
        pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
        cur.execute("""
            INSERT INTO users (username, password_hash, full_name, role)
            VALUES (?, ?, ?, ?)
        """, ("admin", pw_hash, "Administrator", "admin"))

    # ✅ Commit + fermeture propre
    conn.commit()
    conn.close()




def ensure_client_assignments():
    """Vérifie si des clients sont assignés et en ajoute si nécessaire"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Vérifier si des assignations existent
    cursor.execute("SELECT COUNT(*) FROM client_assignments")
    count = cursor.fetchone()[0]
    
    # Si aucune assignation n'existe, en créer quelques-unes pour test
    if count == 0:
        # Récupérer quelques clients de la table forecasts
        cursor.execute("SELECT DISTINCT ship_to_key FROM forecasts LIMIT 10")
        clients = cursor.fetchall()
        
        # Récupérer les utilisateurs avec rôle 'user'
        cursor.execute("SELECT id FROM users WHERE role = 'user'")
        users = cursor.fetchall()
        
        # Si aucun utilisateur 'user', utiliser l'admin
        if not users:
            cursor.execute("SELECT id FROM users WHERE role = 'admin'")
            users = cursor.fetchall()
        
        if clients and users:
            # Assigner les clients aux utilisateurs
            for i, client in enumerate(clients):
                user_id = users[i % len(users)][0]  # Répartir les clients entre les utilisateurs
                try:
                    cursor.execute("""
                        INSERT INTO client_assignments 
                        (sales_rep_id, ship_to_key, ship_to_code, ship_to_name) 
                        VALUES (?, ?, ?, ?)
                    """, (user_id, client[0], f"CODE_{client[0]}", f"Client {client[0]}"))
                except sqlite3.IntegrityError:
                    pass  # Ignorer les doublons
            
            conn.commit()
            print(f"✅ {len(clients)} clients assignés automatiquement pour test")
    
    conn.close()



def get_forecast_history(forecast_id=None, limit=100):
    """
    Récupère l'historique des modifications depuis la base de données.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        base_query = """
            SELECT 
                fh.forecast_id,
                f.product_line,
                f.country,
                fh.field_name,
                fh.old_value,
                fh.new_value,
                u.full_name AS modified_by,
                fh.modified_at
            FROM forecast_history fh
            LEFT JOIN forecasts f ON fh.forecast_id = f.id
            LEFT JOIN users u ON fh.modified_by = u.id
        """

        params = []
        if forecast_id:
            base_query += " WHERE fh.forecast_id = ?"
            params.append(forecast_id)

        base_query += " ORDER BY fh.modified_at DESC LIMIT ?"
        params.append(limit)

        history_df = pd.read_sql(base_query, conn, params=params)

        if 'modified_at' in history_df.columns:
            history_df['modified_at'] = pd.to_datetime(history_df['modified_at'])

        return history_df

    except Exception as e:
        st.error(f"❌ Erreur lors de la récupération de l'historique : {str(e)}")
        return pd.DataFrame()

    finally:
        if 'conn' in locals():
            conn.close()

def safe_parse_json(x):
    try:
        if isinstance(x, dict):
            return x
        if isinstance(x, str) and x.strip() not in ("", "null", "nan"):
            return json.loads(x)
    except Exception:
        pass
    return {}



@st.cache_data(ttl=600)
def get_forecasts(filters: Optional[Dict[str, any]] = None) -> pd.DataFrame:
    """
    Récupère les prévisions de la base SQLite et prépare le DataFrame.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT f.*, u.username AS modified_by_name
            FROM forecasts f
            LEFT JOIN users u ON f.modified_by = u.id
        """
        params = []

        # Gestion des filtres dynamiques
        if filters:
            conds = []
            for col, val in filters.items():
                if col in ("date_from", "date_to"):
                    op = ">=" if col == "date_from" else "<="
                    conds.append(f"date {op} ?")
                    params.append(val)
                elif val not in (None, ""):
                    conds.append(f"{col} = ?")
                    params.append(val)
            if conds:
                query += " WHERE " + " AND ".join(conds)

        # Lecture base ➔ DataFrame
        df = pd.read_sql(query, conn, params=params)
        
        # 🔄 Recalcul des totaux annuels
        fcst_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("ACTUAL & FCST")]
        if fcst_cols:
            df['full_year_forecast'] = df[fcst_cols].sum(axis=1)

        if 'budget_dd' in df.columns:
            df['full_year_budget'] = df['budget_dd'].apply(
                lambda d: sum(d.values()) if isinstance(d, dict) else 0
            )

        # 🔧 Convertir colonnes dates
        for date_col in ["date", "modified_at"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        # 🔧 Générer year / month / year_month
        if "date" in df.columns:
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["year_month"] = df["date"].dt.to_period("M").astype(str)

        # 🔧 Parser colonnes JSON
        json_cols = [
            "actual_fcst", "last_year_actual", "orderbook",
            "budget_dd", "backlog_variation"
        ]
        for col in json_cols:
            if col in df.columns:
                # 1) Remplacer les NaN par "{}"
                # 2) Tenter de parser les chaînes JSON valides
                # 3) Si le résultat n'est pas un dict, retourner {}
                df[col] = (
                    df[col]
                    .fillna("{}")
                    .map(lambda x: json.loads(x)
                         if isinstance(x, str) and x.strip().lower() not in ("", "null") 
                         else x)
                    .map(lambda d: d if isinstance(d, dict) else {})
                )

        # 🔄 Recalcul des totaux annuels après désérialisation JSON
        fcst_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("ACTUAL & FCST")]
        if fcst_cols:
            df['full_year_forecast'] = df[fcst_cols].sum(axis=1)

        if 'budget_dd' in df.columns:
            df['full_year_budget'] = df['budget_dd'].apply(
                lambda d: sum(d.values()) if isinstance(d, dict) else 0
            )

        # 🔧 Conversion numérique robuste
        num_cols = [
            "value", "full_year_forecast", "full_year_budget",
            "forecast_vs_budget", "current_vs_initial",
            "confidence", "firm_order", "delivery_week"
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # 🔧 Compléter colonnes texte manquantes
        for col in ("client_type", "sales_rep"):
            if col in df.columns:
                df[col] = df[col].fillna("Inconnu")
                
        # 🆕 Récupérer les règles d'emballage depuis la base de données
        try:
            packaging_rules_df = pd.read_sql("SELECT * FROM packaging_rules", conn)
            
            # Initialiser les colonnes d'emballage avec des valeurs par défaut
            df['moq_kg'] = 1000  # MOQ par défaut de 1 tonne
            df['pallet_kg'] = 1000  # Poids standard d'une palette
            df['box_kg'] = 25  # Poids standard d'une boîte
            df['boxes_per_pallet'] = 40  # Nombre de boîtes par palette
            
            if not packaging_rules_df.empty:
                # Créer un dictionnaire pour un accès rapide aux règles par code produit
                rules_dict = {}
                for _, rule in packaging_rules_df.iterrows():
                    key = (rule['site_code'], rule['product_code'])
                    rules_dict[key] = {
                        'moq_kg': rule['moq_kg'],
                        'pallet_kg': rule['pallet_size_kg'],
                        'box_kg': rule['packing_size_kg'],
                        'boxes_per_pallet': int(rule['pallet_size_kg'] / rule['packing_size_kg']) if rule['packing_size_kg'] > 0 else 0
                    }
                
                # Pour chaque ligne de prévision, chercher les règles correspondantes
                for idx, row in df.iterrows():
                    site_code = row.get('site_code', 'DEFAULT')
                    product_code = row.get('material_code', '')
                    
                    # Essayer d'abord avec le site spécifique
                    key = (site_code, product_code)
                    if key in rules_dict:
                        rule = rules_dict[key]
                        df.at[idx, 'moq_kg'] = rule['moq_kg']
                        df.at[idx, 'pallet_kg'] = rule['pallet_kg']
                        df.at[idx, 'box_kg'] = rule['box_kg']
                        df.at[idx, 'boxes_per_pallet'] = rule['boxes_per_pallet']
                    else:
                        # Essayer avec le site par défaut
                        key = ('DEFAULT', product_code)
                        if key in rules_dict:
                            rule = rules_dict[key]
                            df.at[idx, 'moq_kg'] = rule['moq_kg']
                            df.at[idx, 'pallet_kg'] = rule['pallet_kg']
                            df.at[idx, 'box_kg'] = rule['box_kg']
                            df.at[idx, 'boxes_per_pallet'] = rule['boxes_per_pallet']
        except Exception as e:
            # En cas d'erreur, utiliser les règles par défaut basées sur product_line
            if 'product_line' in df.columns:
                # Produits chimiques - palettes plus lourdes
                mask_chemical = df['product_line'].str.contains('Chemical', case=False, na=False)
                df.loc[mask_chemical, 'moq_kg'] = 2000
                df.loc[mask_chemical, 'pallet_kg'] = 1200
                df.loc[mask_chemical, 'box_kg'] = 30
                df.loc[mask_chemical, 'boxes_per_pallet'] = 40
                
                # Produits pharmaceutiques - palettes plus légères
                mask_pharma = df['product_line'].str.contains('Pharma', case=False, na=False)
                df.loc[mask_pharma, 'moq_kg'] = 500
                df.loc[mask_pharma, 'pallet_kg'] = 800
                df.loc[mask_pharma, 'box_kg'] = 20
                df.loc[mask_pharma, 'boxes_per_pallet'] = 40
                
                # Produits biologiques - petites quantités
                mask_bio = df['product_line'].str.contains('Bio|Organic', case=False, na=False)
                df.loc[mask_bio, 'moq_kg'] = 250
                df.loc[mask_bio, 'pallet_kg'] = 500
                df.loc[mask_bio, 'box_kg'] = 12.5
                df.loc[mask_bio, 'boxes_per_pallet'] = 40

        # 🔧 Tri de sortie
        if "date" in df.columns:
            df = df.sort_values(["product_line", "country", "date"])

        # ✅ Important pour le tableau collaboratif : reset_index()
        return df.reset_index(drop=True)

    except Exception as e:
        st.error(f"❌ Erreur get_forecasts : {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()


   

def authenticate(username, password, remember=False):
    """
    Authentifie un utilisateur
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        cur.execute("""
            SELECT id, username, full_name, role 
            FROM users 
            WHERE username = ? AND password_hash = ?
        """, (username, password_hash))

        user = cur.fetchone()
        
        if user:
            if remember:
                remember_token = hashlib.sha256(f"{username}{datetime.now()}".encode()).hexdigest()
                expiry = datetime.now() + timedelta(days=30)
                cur.execute("""
                    UPDATE users 
                    SET remember_token = ?, token_expiry = ? 
                    WHERE id = ?
                """, (remember_token, expiry, user[0]))
                conn.commit()
            else:
                remember_token = None
            
            user_data = {
                'id': user[0],
                'username': user[1],
                'full_name': user[2],
                'role': user[3],
                'remember_token': remember_token
            }
            return user_data
        
        return None
    
    finally:
        if 'conn' in locals():
            conn.close()

def quote_sqlite_identifier(name: str) -> str:
    """
    Entoure un nom de colonne avec des guillemets doubles pour l'utiliser en SQL en toute sécurité.
    """
    return '"' + name.replace('"', '""') + '"'


def _numbers_are_equal(new_value, old_value):
    """Vérifie si deux valeurs numériques sont égales avec une tolérance."""
    try:
        return abs(float(new_value) - float(old_value)) < 1e-10
    except (ValueError, TypeError):
        return False


def save_forecast_changes(updated_df, original_df):
    """
    Sauvegarde les lignes modifiées dans la base SQLite avec historique.
    Retourne la liste des IDs des prévisions qui ont été modifiées.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    changed_ids = []
    try:
        # Débogage pour voir ce qui se passe
        st.write("Débogage save_forecast_changes:")
        st.write(f"Types - updated_df: {type(updated_df)}, original_df: {type(original_df)}")
        st.write(f"Colonnes - updated_df: {updated_df.columns.tolist()}")
        st.write(f"Nombre de lignes - updated_df: {len(updated_df)}, original_df: {len(original_df)}")

        user_id = st.session_state.get("user", {}).get("id")
        if not user_id:
            st.error("Utilisateur non authentifié.")
            return []

        json_columns = ["actual_fcst", "last_year_actual", "orderbook", "budget_dd", "backlog_variation"]
        ignored_columns = [
            "id", "modified_at", "modified_by_name", "_original", "previous_value",
            "tooltip_value", "alert_color", "advanced_tooltip", "tooltip_info"
        ]

        # Forcer la conversion des colonnes numériques
        for col in updated_df.columns:
            if col.startswith("ACTUAL & FCST"):
                updated_df[col] = pd.to_numeric(updated_df[col], errors='coerce')
                original_df[col] = pd.to_numeric(original_df[col], errors='coerce')

        # Boucle sur toutes les lignes
        for _, updated_row in updated_df.iterrows():
            row_id = updated_row.get("id")
            if row_id is None:
                continue

            original_rows = original_df[original_df["id"] == row_id]
            if original_rows.empty:
                continue
            original_row = original_rows.iloc[0]

            # Comparer colonne par colonne
            for column in updated_df.columns:
                if column in ignored_columns or column not in original_df.columns:
                    continue

                new_value = updated_row[column]
                old_value = original_row[column]

                # Normalisation JSON
                if column in json_columns:
                    new_value = json.dumps(new_value) if isinstance(new_value, dict) else str(new_value)
                    old_value = json.dumps(old_value) if isinstance(old_value, dict) else str(old_value)

                # Comparaison robuste
                if pd.isna(new_value) and pd.isna(old_value):
                    continue
                if _numbers_are_equal(new_value, old_value):
                    continue
                if str(new_value).strip() == str(old_value).strip():
                    continue

                # Historisation
                cur.execute("""
                    INSERT INTO forecast_history (forecast_id, field_name, old_value, new_value, modified_by, modified_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (row_id, column, str(old_value), str(new_value), user_id))

                # Mise à jour
                safe_col = quote_sqlite_identifier(column)
                cur.execute(f"""
                    UPDATE forecasts
                    SET {safe_col} = ?, modified_by = ?, modified_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_value, user_id, row_id))

                changed_ids.append(row_id)

        if changed_ids:
            conn.commit()
            return changed_ids

        # Si aucun changement détecté, forcer la mise à jour de toutes les lignes
        st.warning("Aucun changement détecté automatiquement. Forçage de la mise à jour de toutes les lignes...")
        forced_ids = []
        for _, row in updated_df.iterrows():
            row_id = row.get("id")
            if row_id is not None:
                cur.execute("""
                    UPDATE forecasts
                    SET modified_by = ?, modified_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (user_id, row_id))
                forced_ids.append(row_id)
        conn.commit()
        return forced_ids

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Erreur lors de la sauvegarde : {e}")
        import traceback
        st.code(traceback.format_exc())
        return []
    finally:
        conn.close()






def get_comments(forecast_id):
    """
    Récupère les commentaires d'une prévision
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        
        comments_df = pd.read_sql("""
            SELECT 
                fc.comment_text,
                u.full_name as user_name,
                fc.created_at
            FROM forecast_comments fc
            JOIN users u ON fc.user_id = u.id
            WHERE fc.forecast_id = ?
            ORDER BY fc.created_at DESC
        """, conn, params=[forecast_id])
        
        return comments_df
    finally:
        if 'conn' in locals():
            conn.close()

def check_remember_token(token):
    """
    Vérifie la validité du token de connexion
    """
    if not token:
        return None
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, username, full_name, role
            FROM users
            WHERE remember_token = ? AND token_expiry > datetime('now')
        """, (token,))
        
        user = cur.fetchone()
        
        if user:
            return {
                'id': user[0],
                'username': user[1],
                'full_name': user[2],
                'role': user[3],
                'remember_token': token
            }
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def validate_forecast_data(forecast_data):
    """
    Valide les données de prévision avant insertion/mise à jour
    """
    errors = []
    
    # Validation des champs JSON
    for field in ['actual_fcst', 'orderbook', 'budget_dd']:
        if field in forecast_data:
            is_valid, error = validate_json_data(forecast_data[field], field)
            if not is_valid:
                errors.append(f"Erreur dans {field}: {error}")
    
    # Validation des valeurs numériques
    numeric_fields = ['value', 'full_year_forecast', 'full_year_budget']
    for field in numeric_fields:
        if field in forecast_data:
            try:
                float(forecast_data[field])
            except (ValueError, TypeError):
                errors.append(f"Le champ {field} doit être numérique")
    
    return len(errors) == 0, errors
# -----------------------------------------------------------------------------
# RENDERING FUNCTIONS
# -----------------------------------------------------------------------------
def render_fancy_header(title, subtitle=None, icon=None):
    """
    Affiche un header stylisé
    """
    header_html = f"""
    <div class="fancy-header">
        <h1>{f'{icon} ' if icon else ''}{title}</h1>
        {f'<p>{subtitle}</p>' if subtitle else ''}
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)

def render_login():
    """
    Affiche la page de connexion
    """
    render_fancy_header("SEQENS Analytics", "Connexion", "🔐")
    
    with st.container():
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            with stylable_container(
                key="login_form",
                css_styles="""
                    {
                        background-color: white;
                        padding: 20px;
                        border-radius: 10px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                """
            ):
                username = st.text_input("👤 Nom d'utilisateur")
                password = st.text_input("🔑 Mot de passe", type="password")
                remember = st.checkbox("Se souvenir de moi")
                
                if st.button("Se connecter", use_container_width=True):
                    if username and password:
                        user = authenticate(username, password, remember)
                        if user:
                            st.session_state.authenticated = True
                            st.session_state.user = user
                            st.session_state.remember_me = remember
                            st.success("✅ Connexion réussie!")
                            st.rerun()
                        else:
                            st.error("❌ Identifiants incorrects")
                    else:
                        st.warning("⚠️ Veuillez saisir vos identifiants")
def add_comment(forecast_id, comment_text, user_id):
    """
    Ajoute un commentaire à une prévision
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO forecast_comments (forecast_id, comment_text, user_id)
            VALUES (?, ?, ?)
        """, (forecast_id, comment_text, user_id))
        
        conn.commit()
        return True
        
    except Exception as e:
        st.error(f"Erreur lors de l'ajout du commentaire : {str(e)}")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()

def render_user_bar():
    """
    Affiche la barre utilisateur
    """
    col1, col2 = st.columns([6,1])
    with col1:
        st.markdown(
            f"<div style='padding: 8px;'>👤 {st.session_state.user['full_name']} "
            f"({st.session_state.user['role']})</div>",
            unsafe_allow_html=True
        )
    with col2:
        if st.button("📤 Déconnexion"):
            st.session_state.authenticated = False
            st.session_state.user = None
            st.session_state.remember_me = False
            st.rerun()

def render_comments_section(forecast_id):
    """
    Affiche la section des commentaires pour une prévision
    """
    with st.expander("💬 Commentaires", expanded=False):
        # Formulaire d'ajout de commentaire
        new_comment = st.text_area("Ajouter un commentaire")
        if st.button("Publier"):
            if new_comment:
                if add_comment(forecast_id, new_comment, st.session_state.user['id']):
                    st.success("Commentaire ajouté!")
                    st.rerun()
            else:
                st.warning("Le commentaire ne peut pas être vide")
        
        # Affichage des commentaires existants
        comments = get_comments(forecast_id)
        if not comments.empty:
            for _, comment in comments.iterrows():
                with stylable_container(
                    key=f"comment_{_}",
                    css_styles="""
                        {
                            background-color: #f0f2f6;
                            padding: 10px;
                            border-radius: 5px;
                            margin: 5px 0;
                        }
                    """
                ):
                    st.markdown(f"**{comment['user_name']}** - {comment['created_at']}")
                    st.markdown(comment['comment_text'])

                    
def render_collaborative_table():
    """Tableau collaboratif avec synchronisation Google Sheets, surbrillance, alertes, historique et commentaires internes"""
    import plotly.express as px
    from datetime import datetime
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
    from google_sync import add_forecast_row_to_sheet
    import re

    st.markdown("### 📋 Tableau collaboratif")

        # --- 1. CHARGEMENT DES DONNÉES ---
    original_df = get_forecasts()
    if "id" not in original_df.columns:
        st.error("❌ La colonne 'id' est absente des données. Impossible de tracer les modifications.")
        st.stop()

    if 'original_df' not in st.session_state:
        st.session_state.original_df = original_df.copy()

        # --- FILTRAGE PAR CLIENTS ASSIGNÉS ---
    # Les administrateurs voient tout, les utilisateurs ne voient que leurs clients
    if st.session_state.user["role"] != "admin":
        # Récupérer les clients assignés à l'utilisateur connecté
        conn = sqlite3.connect(DB_PATH)
        user_clients = pd.read_sql("""
            SELECT ship_to_key, ship_to_name, ship_to_code
            FROM client_assignments
            WHERE sales_rep_id = ?
        """, conn, params=[st.session_state.user["id"]])
        conn.close()
        
        # Si l'utilisateur a des clients assignés, filtrer les données
        if not user_clients.empty:
            # Filtrer par ship_to_key (identifiant unique) ou par ship_to_name (nom du client)
            client_keys = user_clients["ship_to_key"].tolist()
            client_names = user_clients["ship_to_name"].tolist()
            client_codes = user_clients["ship_to_code"].tolist()
            
            # Créer un masque pour filtrer les données
            mask = (
                original_df["ship_to_key"].isin(client_keys) | 
                original_df["ship_to_name"].isin(client_names) |
                original_df["ship_to_code"].isin(client_codes)
            )
            
            original_df = original_df[mask]
            st.info(f"📋 Affichage de {len(original_df)} lignes correspondant à vos {len(user_clients)} clients assignés.")
        else:
            st.warning("⚠️ Aucun client ne vous est assigné. Contactez un administrateur.")
            # Afficher un DataFrame vide si aucun client n'est assigné
            original_df = original_df.head(0)
        
        # Mettre à jour la session state
        st.session_state.original_df = original_df.copy()





    def is_forecast_column(col: str) -> bool:
        """
        Détecte si une colonne suit le format 'ACTUAL & FCST YYYY/MM' exactement.
        """
        col_str = str(col).strip()
        return bool(re.match(r"(?i)^ACTUAL\s*&\s*FCST\s+\d{4}/\d{2}$", col_str))

    forecast_cols = [col for col in original_df.columns if is_forecast_column(col)]

    if not forecast_cols:
        st.warning("⚠️ Aucune colonne mensuelle (format 'ACTUAL & FCST YYYY/MM') détectée dans les données.")
    else:
        st.success(f"✅ {len(forecast_cols)} colonne(s) mensuelle(s) détectée(s).")

        # --- 3. ÉDITION MULTI-COLONNES ---
    # Toutes les colonnes listées dans `forecast_cols` seront rendues éditables dans la grille AgGrid (pas de sélection unique)
    # Mais on garde une référence au premier mois pour l'affichage des détails
    selected_month = forecast_cols[0] if forecast_cols else None

                # --- 4. FILTRES POUR LES DONNÉES ---
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
    
    # Récupérer les clients assignés au vendeur connecté
    user_id = st.session_state.user["id"]
    user_role = st.session_state.user["role"]
    
    # Par défaut, filtrer pour n'afficher que les clients du vendeur (sauf pour les admins)
    show_all_clients = False
    
    # Récupérer les clients assignés au vendeur
    assigned_clients = []
    if user_role != "admin":
        try:
            conn = sqlite3.connect(DB_PATH)
            client_query = """
                SELECT ship_to_key 
                FROM client_assignments 
                WHERE sales_rep_id = ?
            """
            assigned_df = pd.read_sql(client_query, conn, params=[user_id])
            conn.close()
            
            if not assigned_df.empty:
                assigned_clients = assigned_df['ship_to_key'].tolist()
        except Exception as e:
            st.warning(f"Impossible de récupérer les clients assignés: {e}")
    
    with col1:
        # Option pour basculer entre "Mes clients" et "Tous les clients"
        if user_role != "admin" and assigned_clients:
            show_all_clients = st.checkbox("👥 Afficher tous les clients", value=False)
            
            if not show_all_clients:
                st.info(f"📋 Affichage de vos {len(assigned_clients)} clients assignés")
    
    with col2:
        # Filtre par client spécifique
        # Filtrer les valeurs None avant de trier
        all_clients = [c for c in original_df['ship_to_key'].unique() if c is not None]
        all_clients.sort()  # Trier après avoir filtré les None
        
        all_client_names = {}
        
        # Créer un dictionnaire pour afficher les noms des clients
        for idx, row in original_df.iterrows():
            if ('ship_to_key' in row and row['ship_to_key'] is not None and 
                'ship_to_name' in row and row['ship_to_name'] is not None):
                all_client_names[row['ship_to_key']] = str(row['ship_to_name'])
        
        # Fonction pour formater l'affichage des clients
        def format_client(client_key):
            if client_key is None:
                return "Client inconnu"
            name = all_client_names.get(client_key, '')
            return f"{client_key} - {name}" if name else f"{client_key}"
        
        # Filtre de clients avec tous les clients disponibles
        client_filter_specific = st.multiselect(
            "🏢 Client spécifique",
            options=all_clients,
            default=[],
            format_func=format_client
        )
    
    with col3:
        product_filter = st.multiselect(
            "🧪 Filtrer par produit",
            options=sorted([p for p in original_df['product_line'].unique() if p is not None]),
            default=[]
        )

    with col4:
        country_filter = st.multiselect(
            "🌍 Filtrer par pays",
            options=sorted([c for c in original_df['country'].unique() if c is not None]),
            default=[]
        )
    
    with col5:
        if 'client_type' in original_df.columns:
            client_options = [
                c for c in original_df['client_type'].unique()
                if c is not None and str(c).strip() != ""
            ]
            client_filter = st.multiselect(
                "🏢 Filtrer par type de client",
                options=sorted(client_options) if client_options else [],
                default=[]
            )
        else:
            client_filter = []

    # Appliquer les filtres
    filtered_df = original_df.copy()
    
    # Filtrer par clients assignés si nécessaire
    if user_role != "admin" and assigned_clients and not show_all_clients:
        filtered_df = filtered_df[filtered_df['ship_to_key'].isin(assigned_clients)]
    
    # Filtrer par client spécifique si sélectionné
    if client_filter_specific:
        filtered_df = filtered_df[filtered_df['ship_to_key'].isin(client_filter_specific)]
    
    # Appliquer les autres filtres
    if product_filter:
        filtered_df = filtered_df[filtered_df['product_line'].isin(product_filter)]
    if country_filter:
        filtered_df = filtered_df[filtered_df['country'].isin(country_filter)]
    if 'client_type' in original_df.columns and client_filter:
        filtered_df = filtered_df[filtered_df['client_type'].isin(client_filter)]


                   # --- 5. COLONNES À AFFICHER ---
    st.write("### 📋 Colonnes à afficher")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

    with col1:
        show_all = st.checkbox("📊 Afficher toutes les colonnes", value=False)
    
    with col2:
        show_all_months = st.checkbox("📅 Afficher les 12 mois", value=False)
    
    with col3:
        # Sélection d'un mois supplémentaire à afficher
        # Trier d'abord les colonnes mensuelles
        forecast_cols_sorted = sorted(
            forecast_cols,
            key=lambda c: pd.to_datetime(c.replace("ACTUAL & FCST ", "") + "-01")
        )
        # Puis créer la liste des options pour le mois supplémentaire
        additional_month_options = ["Aucun"] + [col for col in forecast_cols_sorted if col not in forecast_cols_sorted[:6]]
        additional_month = st.selectbox(
            "➕ Mois supplémentaire",
            options=additional_month_options,
            format_func=lambda x: x.replace("ACTUAL & FCST ", "") if x != "Aucun" else x
        )
    with col4:
    # Sélection des colonnes d'emballage à afficher
        packaging_columns = ["moq_kg", "pallet_kg", "box_kg", "boxes_per_pallet"]
        packaging_labels = {
        "moq_kg": "📦 MOQ (kg)",
        "pallet_kg": "🔢 Palette (kg)",
        "box_kg": "📦 Boîte (kg)",
        "boxes_per_pallet": "🧮 Boîtes/Palette"
    }
        
    # 1) Colonnes toujours visibles
    default_display_cols = [
        "ship_to_key",
        "ship_to_name",
        "material_description"
]
     
    packaging_cols_to_show = st.multiselect(
        "📦 Colonnes d'emballage",
        options=packaging_columns,
        default=[],
        format_func=lambda x: packaging_labels.get(x, x)
    )

    # 2) Les mois de forecast
    # Utiliser forecast_cols_sorted déjà défini ci-dessus
    if show_all_months:
        # Tous les 12 mois
        months_to_display = forecast_cols_sorted
    else:
        # Les 6 premiers mois par défaut
        first_6_months = forecast_cols_sorted[:6]
        months_to_display = first_6_months
    
           # Ajouter les mois au display
    default_display_cols += months_to_display
    
    # Ajouter le mois supplémentaire s'il est sélectionné
    if additional_month != "Aucun" and additional_month not in months_to_display:
        default_display_cols.append(additional_month)

    # 3) Ajouter les colonnes full-year (toujours visibles)
    full_year_cols = []
    for col in ["full_year_budget", "full_year_forecast"]:
        if col in filtered_df.columns:
            full_year_cols.append(col)
            default_display_cols.append(col)

    # Préparer les colonnes d'emballage sélectionnées
    packaging_cols_filtered = [col for col in packaging_cols_to_show if col in filtered_df.columns]
    
    # Si des colonnes d'emballage sont sélectionnées, les insérer avant les colonnes full-year
    if packaging_cols_filtered:
        # Retirer les colonnes full-year
        temp_cols = [col for col in default_display_cols if col not in full_year_cols]
        # Reconstruire avec les colonnes d'emballage avant les full-year
        default_display_cols = temp_cols + packaging_cols_filtered + full_year_cols

    # 4) Construire la liste finale
    if show_all:
        # on affiche tout (sauf les tech cols)
        display_cols = [
            c for c in filtered_df.columns 
            if c not in ["tooltip_info", "advanced_tooltip"]
        ]
    else:
        # on ne garde que celles de default_display_cols qui existent
        display_cols = [c for c in default_display_cols if c in filtered_df.columns]

    # 5) Toujours ajouter l'ID pour le tracking
    if "id" not in display_cols:
        display_cols.append("id")

    # 6) Nouveau DataFrame
    df_display = filtered_df[display_cols].copy()







    # --- 6. PRÉPARATION DES TOOLTIPS ---
    # Tooltip simple pour l'affichage au survol
    def safe_tooltip_generator(row):
        try:
            if 'ship_to_key' not in row or 'material_description' not in row:
                return "Information non disponible"
            
            # Récupérer les informations pour le tooltip
            orderbook = row.get("ORDERBOOK 2025/05", "N/A")
            budget = row.get("BUDGET DD 2025/05 VOL (To)", "N/A")
            backlog = row.get("BACKLOG VARIATION 2025/05", "N/A")
            
            tooltip = f"""
            📦 Commande en cours: {orderbook}
            💰 Budget: {budget}
            📊 Historique: {backlog}
            """
            
            return tooltip
        except Exception:
            return "Information non disponible"
    
    df_display["tooltip_info"] = df_display.apply(safe_tooltip_generator, axis=1)
    
       # Tooltip avancé pour l'affichage au clic - avec gestion des erreurs
    def safe_advanced_tooltip_generator(row):
        try:
            # Récupérer l'ID de la ligne pour chercher les données complètes
            row_id = row.get("id")
            if row_id is not None:
                # Récupérer la ligne complète depuis le DataFrame original
                full_row = filtered_df[filtered_df["id"] == row_id].iloc[0] if not filtered_df[filtered_df["id"] == row_id].empty else row
            else:
                full_row = row
                
            # Historique des 3 derniers mois - avec gestion des erreurs
            history = []
            try:
                if 'ship_to_key' in full_row and 'material_description' in full_row:
                    # Récupérer l'historique des modifications pour cette ligne
                    conn = sqlite3.connect(DB_PATH)
                    history_query = """
                        SELECT field_name, old_value, new_value, modified_at, u.full_name
                        FROM forecast_history fh
                        JOIN users u ON fh.modified_by = u.id
                        WHERE forecast_id = ?
                        ORDER BY modified_at DESC
                        LIMIT 3
                    """
                    
                    history_df = pd.read_sql(history_query, conn, params=[full_row["ship_to_key"]])
                    conn.close()
                    
                    if not history_df.empty:
                        for _, hist_row in history_df.iterrows():
                            date_str = pd.to_datetime(hist_row['modified_at']).strftime("%Y-%m-%d %H:%M")
                            history.append({
                                "date": date_str,
                                "user": hist_row['full_name'],
                                "field": hist_row['field_name'],
                                "old_value": hist_row['old_value'],
                                "new_value": hist_row['new_value']
                            })
            except Exception:
                pass
            
            # Commandes en cours (orderbook)
            orders = {}
            try:
                orderbook_data = full_row.get("orderbook", "{}")
                # Convertir la chaîne JSON en dictionnaire si nécessaire
                if isinstance(orderbook_data, str):
                    orderbook_data = json.loads(orderbook_data)
                if isinstance(orderbook_data, dict):
                    orders = {k: v for k, v in orderbook_data.items() if isinstance(v, (int, float)) and v > 0}
            except Exception:
                pass
            
            # Budget mensuel (budget_dd)
            budget = {}
            try:
                budget_data = full_row.get("budget_dd", "{}")
                # Convertir la chaîne JSON en dictionnaire si nécessaire
                if isinstance(budget_data, str):
                    budget_data = json.loads(budget_data)
                if isinstance(budget_data, dict):
                    budget = {k: v for k, v in budget_data.items() if isinstance(v, (int, float))}
            except Exception:
                pass
            
            # Historique des variations de backlog
            backlog = {}
            try:
                backlog_data = full_row.get("backlog_variation", "{}")
                # Convertir la chaîne JSON en dictionnaire si nécessaire
                if isinstance(backlog_data, str):
                    backlog_data = json.loads(backlog_data)
                if isinstance(backlog_data, dict):
                    backlog = {k: v for k, v in backlog_data.items() if isinstance(v, (int, float))}
            except Exception:
                pass
            
            return {
                "history": history,
                "orders": orders,
                "budget": budget,
                "backlog": backlog
            }
        except Exception:  # Ajout du bloc except manquant
            return {
                "history": [],
                "orders": {},
                "budget": {},
                "backlog": {}
            }




    
    # Convertir en JSON pour le passer à JavaScript
    df_display["advanced_tooltip"] = df_display.apply(
        lambda x: json.dumps(safe_advanced_tooltip_generator(x), default=str),
        axis=1
    )

        # --- 7. CONFIGURATION DU STYLE DU TABLEAU ---
    # Définir un thème personnalisé pour le tableau
    custom_css = {
        ".ag-header-cell": {
            "background-color": "#1f77b4 !important",
            "color": "white !important",
            "font-weight": "bold !important",
            "font-size": "15px !important",  # Augmenté de 14px à 15px
            "padding": "8px !important"
        },
        ".ag-cell": {
            "font-size": "14px !important",  # Taille de police pour toutes les cellules
            "padding": "4px 8px !important"  # Plus d'espace dans les cellules
        },
        ".ag-row-odd": {"background-color": "#f8f9fa !important"},
        ".ag-row-even": {"background-color": "#ffffff !important"},
        ".ag-row-hover": {"background-color": "#e9f5ff !important"},
        ".ag-row-selected": {"background-color": "#d1e7ff !important"},
        ".ag-cell-focus": {"border": "1px solid #0d6efd !important"},
        ".ag-cell-editable": {
            "background-color": "#f0f8ff !important",
            "font-weight": "bold !important",  # Mettre en gras les cellules éditables
            "color": "#0d6efd !important"      # Couleur bleue pour les valeurs éditables
        }
    }


       # Fonction pour colorer les cellules selon leur valeur
    cell_style_jscode = JsCode("""
function(params) {
    try {
        // Coloration des valeurs
        if (params.colDef.field.startsWith('ACTUAL & FCST')) {
            // Mettre en évidence les cellules éditables pour les vendeurs
            if (params.colDef.editable) {
                // Vérifier si la valeur est inférieure à la MOQ (Minimum Order Quantity)
                const value = parseFloat(params.value);
                const moq = params.data.moq_kg;
                
                if (!isNaN(value) && value > 0 && moq && value < moq) {
                    // Alerte si commande inférieure à la MOQ
                    return {
                        'backgroundColor': '#fff3cd', 
                        'fontWeight': 'bold', 
                        'border': '1px solid #ffc107',
                        'fontSize': '14px',
                        'textAlign': 'right',
                        'color': '#856404'
                    };
                }
                
                return {
                    'backgroundColor': '#f0f8ff', 
                    'fontWeight': 'bold', 
                    'border': '1px solid #1f77b4',
                    'fontSize': '14px',
                    'textAlign': 'right'  // Aligner les nombres à droite
                };
            }
        }
        
        // Coloration selon le type de prévision
        if (params.colDef.field === 'forecast_type') {
            if (params.value === 'Initial') {
                return {'color': '#0d6efd', 'fontStyle': 'italic'};
            } else if (params.value === 'Révisé') {
                return {'color': '#fd7e14', 'fontWeight': 'bold'};
            }
        }
        
        // Coloration des écarts budget vs prévision
        if (params.colDef.field === 'forecast_vs_budget') {
            const value = parseFloat(params.value);
            if (!isNaN(value)) {
                if (value > 20) {
                    return {'backgroundColor': '#f8d7da', 'color': '#721c24', 'fontWeight': 'bold'};
                } else if (value < -20) {
                    return {'backgroundColor': '#d4edda', 'color': '#155724', 'fontWeight': 'bold'};
                }
            }
        }
        
        // Mise en évidence des colonnes d'emballage
        if (['moq_kg', 'pallet_kg', 'box_kg', 'boxes_per_pallet'].includes(params.colDef.field)) {
            return {
                'backgroundColor': '#e2f0d9',
                'color': '#2e7d32',
                'fontWeight': 'bold',
                'textAlign': 'right'
            };
        }
    } catch (error) {
        console.error("Erreur dans cell_style_jscode:", error);
    }
    
    return null;
}
""")


    # Renderer pour la colonne value avec icône d'info
    value_renderer_js = JsCode("""
    function(params) {
        try {
            if (params.value === null || params.value === undefined) return '';
            
            // Solution simple : juste la valeur suivie d'un emoji Unicode
            return params.value + ' ℹ️';
        } catch (error) {
            console.error("Erreur dans value_renderer_js:", error);
            return params.value || '';
        }
    }
    """)

            # Code JavaScript pour le tooltip avancé au clic
    cell_click_js = JsCode("""
function(e) {
    try {
        // Récupérer les données de la ligne cliquée
        const data = e.data;
        
        // Vérifier si on a des données avancées
        if (!data || !data.advanced_tooltip) return;
        
        // Parser les données JSON
        let tooltipData;
        try {
            tooltipData = JSON.parse(data.advanced_tooltip);
        } catch (parseError) {
            console.error("Erreur de parsing JSON:", parseError);
            tooltipData = {
                orders: {},
                budget: {},
                backlog: {}
            };
        }
        
        // Créer le contenu HTML du tooltip
        let content = '<div style="background-color: white; border: 1px solid #ddd; padding: 15px; border-radius: 8px; box-shadow: 0 3px 10px rgba(0,0,0,0.2); max-width: 350px; max-height: 400px; overflow-y: auto;">';
        
        // Titre
        const shipToKey = data.ship_to_key || 'ID inconnu';
        const materialDesc = data.material_description || 'Produit inconnu';
        content += `<h4 style="margin-top: 0; color: #1f77b4; border-bottom: 1px solid #eee; padding-bottom: 8px; position: sticky; top: 0; background-color: white; cursor: move;">${shipToKey} - ${materialDesc}</h4>`;
        
        // Commandes en cours
        content += '<h5 style="margin-bottom: 5px; margin-top: 5px; color: #555;">📦 Commandes en cours</h5>';
        if (tooltipData.orders && Object.keys(tooltipData.orders).length > 0) {
            content += '<ul style="margin-top: 0; padding-left: 20px;">';
            Object.entries(tooltipData.orders).forEach(([month, value]) => {
                content += `<li>${month}: <b>${value}</b></li>`;
            });
            content += '</ul>';
        } else {
            content += '<p style="margin: 0; color: #777;">Aucune commande en cours</p>';
        }
        
        // Budget
        content += '<h5 style="margin-bottom: 5px; margin-top: 15px; color: #555;">💰 Budget</h5>';
        if (tooltipData.budget && Object.keys(tooltipData.budget).length > 0) {
            // Filtrer pour ne garder que les valeurs >= 1
            const significantBudget = Object.entries(tooltipData.budget).filter(([_, value]) => value >= 1);
            
            if (significantBudget.length > 0) {
                content += '<ul style="margin-top: 0; padding-left: 20px;">';
                significantBudget.forEach(([month, value]) => {
                    content += `<li>${month}: <b>${value}</b></li>`;
                });
                content += '</ul>';
            } else {
                content += '<p style="margin: 0; color: #777;">Aucun budget significatif</p>';
            }
        } else {
            content += '<p style="margin: 0; color: #777;">Aucun budget disponible</p>';
        }

        // Variations
        content += '<h5 style="margin-bottom: 5px; margin-top: 15px; color: #555;">📊 Variations</h5>';
        if (tooltipData.backlog && Object.keys(tooltipData.backlog).length > 0) {
            // Filtrer pour ne garder que les valeurs dont la valeur absolue est >= 1
            const significantVariations = Object.entries(tooltipData.backlog).filter(([_, value]) => Math.abs(value) >= 1);
            
            if (significantVariations.length > 0) {
                content += '<ul style="margin-top: 0; padding-left: 20px;">';
                significantVariations.forEach(([month, value]) => {
                    content += `<li>${month}: <b>${value}</b></li>`;
                });
                content += '</ul>';
            } else {
                content += '<p style="margin: 0; color: #777;">Aucune variation significative</p>';
            }
        } else {
            content += '<p style="margin: 0; color: #777;">Aucune variation disponible</p>';
        }
        
        content += '</div>';
        
        // Créer et afficher le tooltip
        const tooltip = document.createElement('div');
        tooltip.innerHTML = content;
        tooltip.style.position = 'absolute';
        tooltip.style.zIndex = '1000';
        tooltip.style.left = (e.event.clientX + 10) + 'px';
        tooltip.style.top = (e.event.clientY + 10) + 'px';
        tooltip.id = 'ag-grid-tooltip';
        
        // Supprimer tout tooltip existant
        const existingTooltip = document.getElementById('ag-grid-tooltip');
        if (existingTooltip) {
            existingTooltip.remove();
        }
        
        // Ajouter le nouveau tooltip
        document.body.appendChild(tooltip);
        
        // Rendre le tooltip déplaçable
        let isDragging = false;
        let offsetX, offsetY;
        
        // Fonction pour commencer le déplacement
        function startDrag(e) {
            isDragging = true;
            offsetX = e.clientX - tooltip.getBoundingClientRect().left;
            offsetY = e.clientY - tooltip.getBoundingClientRect().top;
            e.preventDefault();
        }
        
        // Fonction pour déplacer le tooltip
        function dragTooltip(e) {
            if (!isDragging) return;
            tooltip.style.left = (e.clientX - offsetX) + 'px';
            tooltip.style.top = (e.clientY - offsetY) + 'px';
            e.preventDefault();
        }
        
        // Fonction pour arrêter le déplacement
        function stopDrag() {
            isDragging = false;
        }
        
        // Ajouter les écouteurs d'événements pour le déplacement
        const tooltipHeader = tooltip.querySelector('h4');
        if (tooltipHeader) {
            tooltipHeader.addEventListener('mousedown', startDrag);
            document.addEventListener('mousemove', dragTooltip);
            document.addEventListener('mouseup', stopDrag);
        }
        
        // Fermer le tooltip au clic n'importe où (sauf sur le tooltip lui-même)
        document.addEventListener('click', function closeTooltip(event) {
            if (!tooltip.contains(event.target)) {
                tooltip.remove();
                document.removeEventListener('mousemove', dragTooltip);
                document.removeEventListener('mouseup', stopDrag);
                document.removeEventListener('click', closeTooltip);
            }
        });
        
    } catch (error) {
        console.error("Erreur dans le tooltip:", error);
    }
}
""")




                    # --- 8. CONFIGURATION DES COLONNES ---
    gb = GridOptionsBuilder.from_dataframe(df_display)
    
    # Colonnes d'identification (non éditables)
    id_columns = [
        "ship_to_key", "ship_to_name", "ship_to_code",
        "material_code", "material_description", "product_line", "country"
    ]
    
    # Colonnes d'emballage (non éditables)
    packaging_columns = [
        "moq_kg", "pallet_kg", "box_kg", "boxes_per_pallet"
    ]
    
        # Configuration des colonnes
    for col in df_display.columns:
        if col in ["id", "advanced_tooltip", "tooltip_info"]:
            # Colonnes cachées
            gb.configure_column(col, hide=True)

        elif isinstance(col, str) and "ACTUAL & FCST" in col:
            # Extraire le mois et l'année du nom de la colonne
            month_year_match = re.search(r"ACTUAL & FCST (\d{4})/(\d{2})", col)
            if month_year_match:
                year = month_year_match.group(1)
                month = month_year_match.group(2)
                # Format court: MM/YY (par exemple 05/25)
                short_header = f"{month}/{year[-2:]}"
            else:
                short_header = col
                
            gb.configure_column(
                col,
                header_name=f"📝 {short_header}",
                editable=True,                   # toutes éditables
                singleClickEdit=True,
                tooltipField="tooltip_info",
                cellEditor="agNumericCellEditor",
                cellEditorParams={},
                valueParser=JsCode("""
                    function(params) {
                        return parseFloat(params.newValue);
                    }
                """),
                cellStyle=cell_style_jscode
            )

        elif col in id_columns:
            # Colonnes d'identification (non éditables)
            gb.configure_column(
                col,
                editable=False,
                filterable=True,
                sortable=True,
                resizable=True,
                pinned="left"
            )
            
        elif col in packaging_columns:
            # Nouvelles colonnes d'emballage (non éditables)
            icon_map = {
                "moq_kg": "📦 MOQ (kg)",
                "pallet_kg": "🔢 Palette (kg)",
                "box_kg": "📦 Boîte (kg)",
                "boxes_per_pallet": "🧮 Boîtes/Palette"
            }
            header_name = icon_map.get(col, col)
            
            gb.configure_column(
                col,
                header_name=header_name,
                editable=False,
                filterable=True,
                sortable=True,
                resizable=True,
                type="numericColumn",
                valueFormatter=JsCode("""
                    function(params) {
                        if (params.value === null || params.value === undefined) return '';
                        return params.value.toLocaleString();
                    }
                """)
            )

        elif col in ["ORDERBOOK 2025/05", "BUDGET DD 2025/05 VOL (To)", "BACKLOG VARIATION 2025/05"]:
            # Colonnes d'information (non éditables)
            gb.configure_column(
                col,
                tooltipField="tooltip_info",
                editable=False
            )

        else:
            # Colonnes standard (non éditables)
            gb.configure_column(
                col,
                editable=False,
                filterable=True,
                sortable=True,
                resizable=True
            )



    
        # Configuration de la sélection et des options de la grille
    gb.configure_selection("single")
    
        # Configuration de la sélection et des options de la grille
    gb.configure_selection("single")
    
    # Options générales de la grille
    gb.configure_grid_options(
        enableBrowserTooltips=True,
        onCellClicked=cell_click_js,
        rowHeight=40,                     # Lignes plus hautes pour faciliter la lecture
        headerHeight=45,                  # En-têtes plus hauts
        animateRows=True,                 # Animation lors du tri
        enableRangeSelection=True,        # Sélection de plages
        suppressRowClickSelection=False,  # Sélection de ligne au clic
        pagination=True,                  # Pagination pour les grands tableaux
        paginationAutoPageSize=True,      # Taille de page automatique
        enableCellTextSelection=True,
        ensureDomOrder=True,
        stopEditingWhenCellsLoseFocus=True,
        enterMovesDown=False,
        singleClickEdit=True,
        defaultColDef={
            'flex': 1,
            'minWidth': 100,
            'filter': True,
            'resizable': True,
            'sortable': True
        },
        # 🆕 Ajout critique : identifiant unique pour chaque ligne
        getRowId=JsCode("function(params) { return params.data.id; }"),
        onGridReady=JsCode("""
        function(params){
            window._gridApi = params.api;
        }
        """)
    )
    
    # Construction finale des options de grille
    grid_opts = gb.build()


    
                               # --- 9. AFFICHAGE DE LA GRILLE -------------------------------------------
    st.markdown('<div id="grid-wrapper" style="position:relative;">',
                unsafe_allow_html=True)

    from st_aggrid import GridUpdateMode, DataReturnMode

    grid_response = AgGrid(
    df_display,
    
    gridOptions=grid_opts,
    update_mode=GridUpdateMode.VALUE_CHANGED,        # ✅ modèle mis à jour = meilleur déclencheur
    data_return_mode=DataReturnMode.AS_INPUT
,  # ✅ important pour garder toutes les données à jour
    fit_columns_on_grid_load=True,
    theme="streamlit",
    height=700,
    allow_unsafe_jscode=True,
    custom_css=custom_css,
    reload_data=False
)
    

    updated_df = pd.DataFrame(grid_response["data"])
    updated_df["id"] = updated_df["id"].astype(int)  # ← AJOUT TRÈS IMPORTANT
    selected   = grid_response["selected_rows"]


    
    

    st.markdown('</div>', unsafe_allow_html=True)


    # -------------------------------------------------------------------------
    #  Bouton plein‑écran et script   →   même iframe, donc pas de sandbox clash
    # -------------------------------------------------------------------------
    components.html(
        """
        <style>
          #fs-btn{
            position:fixed;
            bottom:20px; right:20px;
            width:42px; height:42px;
            border:none; border-radius:50%;
            background:#1f77b4;  color:#fff;
            font-size:1.3rem; font-weight:bold;
            cursor:pointer;   z-index:1000;
          }
          /* Wrapper en plein‑écran quand on ajoute .fullscreen */
          #grid-wrapper.fullscreen{
            position:fixed !important;
            inset:0 !important;   /* top:0; right:0; bottom:0; left:0 */
            width:100vw !important; height:100vh !important;
            background:#fff;   padding:12px;
            z-index:9999;
          }
          #grid-wrapper.fullscreen .ag-root-wrapper{
            height:100% !important;    /* force Ag‑Grid à remplir */
          }
        </style>

        <!-- Le bouton -->
        <button id="fs-btn" title="Plein écran">⛶</button>

        <script>
          const btn     = document.getElementById("fs-btn");
          const wrapper = window.parent.document.getElementById("grid-wrapper");

          btn.addEventListener("click", ()=> {
              if(!wrapper){ console.error("grid-wrapper introuvable"); return; }
              wrapper.classList.toggle("fullscreen");
          });
        </script>
        """,
        height=0,  # iframe invisible (mais le bouton est en position:fixed donc visible)
        scrolling=False
    )







                                         # --- 10. DÉTECTION ET SAUVEGARDE DES MODIFICATIONS ---
    if st.button("💾 Sauvegarder les modifications", use_container_width=True):
        # Recréer le DataFrame à partir de AgGrid
        updated_df = pd.DataFrame(grid_response["data"])
        # Re-caster l'id pour qu'il soit un entier
        updated_df["id"] = updated_df["id"].astype(int)

        # Afficher le nombre de lignes pour débogage
        st.write(f"Nombre de lignes à traiter : {len(updated_df)}")

        # Récupérer le DataFrame original
        original_df = st.session_state.original_df

        # Vérifier la présence de la colonne id
        if "id" not in updated_df.columns:
            st.error("La colonne `id` est requise pour la sauvegarde.")
        else:
            # Appeler directement la fonction de sauvegarde
            success = save_forecast_changes(updated_df, original_df)

            if success:
                # Mettre à jour la référence originale pour la prochaine comparaison
                st.session_state.original_df = updated_df.copy()
                st.success("✅ Modifications enregistrées avec succès !")
                st.rerun()
            else:
                st.error("❌ Échec de la sauvegarde.")









    # --- HISTORIQUE GLOBAL DÉPLIABLE ---
    with st.expander("📜 Historique des modifications", expanded=False):
        try:
            # Récupération de l'historique global (limité aux 20 dernières modifications)
            conn = sqlite3.connect(DB_PATH)
            history_query = """
                SELECT 
                    fh.forecast_id, 
                    f.ship_to_name,
                    f.material_description,
                    fh.field_name, 
                    fh.old_value, 
                    fh.new_value, 
                    u.full_name,
                    fh.modified_at
                FROM forecast_history fh
                JOIN forecasts f ON fh.forecast_id = f.id
                JOIN users u ON fh.modified_by = u.id
                ORDER BY fh.modified_at DESC
                LIMIT 20
            """
            
            history_df = pd.read_sql(history_query, conn)
            conn.close()
            
            if not history_df.empty:
                # Affichage de l'historique dans un tableau
                history_display = history_df.copy()
                history_display['modified_at'] = pd.to_datetime(history_display['modified_at']).dt.strftime("%Y-%m-%d %H:%M")
                history_display.columns = ['ID', 'Client', 'Produit', 'Champ', 'Ancienne valeur', 'Nouvelle valeur', 'Modifié par', 'Date']
                
                st.dataframe(history_display, use_container_width=True)
            else:
                st.info("Aucun historique de modification disponible.")
                
        except Exception as e:
            st.error(f"❌ Erreur lors de la récupération de l'historique global : {str(e)}")

    # --- 11. AFFICHAGE DES DÉTAILS POUR LA LIGNE SÉLECTIONNÉE ---
    if selected:
        try:
            sel = selected[0]
            st.markdown("----")
            st.subheader(f"Détails pour : {sel.get('ship_to_key', 'N/A')} – {sel.get('material_description', 'N/A')}")

            
            # Affichage des détails sous forme de colonnes
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(f"**ID:** {sel.get('ship_to_key', 'N/A')}")
                st.markdown(f"**Client:** {sel.get('ship_to_name', 'N/A')}")
                st.markdown(f"**Produit:** {sel.get('material_description', 'N/A')}")
            
            with col2:
                if "ORDERBOOK 2025/05" in sel:
                    st.markdown(f"**Commande en cours:** {sel.get('ORDERBOOK 2025/05', 'N/A')}")
                if "BUDGET DD 2025/05 VOL (To)" in sel:
                    st.markdown(f"**Budget:** {sel.get('BUDGET DD 2025/05 VOL (To)', 'N/A')}")
                if "BACKLOG VARIATION 2025/05" in sel:
                    st.markdown(f"**Historique:** {sel.get('BACKLOG VARIATION 2025/05', 'N/A')}")
            
            with col3:
                if selected_month:
                    st.markdown(f"**Valeur {selected_month.replace('ACTUAL & FCST ', '')}:** {sel.get(selected_month, 'N/A')}")
                    
                    # Calcul de l'écart par rapport au budget
                    if 'BUDGET DD 2025/05 VOL (To)' in sel and sel.get(selected_month) is not None:
                        try:
                            budget = float(sel.get('BUDGET DD 2025/05 VOL (To)', 0))
                            value = float(sel.get(selected_month, 0))
                            if budget > 0:
                                variance = ((value - budget) / budget) * 100
                                st.markdown(f"**Écart vs Budget:** {variance:.1f}%")
                                
                                # Indicateur visuel
                                if abs(variance) > 20:
                                    if variance > 0:
                                        st.markdown("⚠️ **Écart significatif au-dessus du budget**")
                                    else:
                                        st.markdown("⚠️ **Écart significatif en-dessous du budget**")
                        except:
                            pass

            # Filtrer de manière sécurisée pour afficher l'historique
            if 'ship_to_key' in sel:
                # Affichage de l'historique complet pour cette ligne
                with st.expander("📜 Historique complet"):
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        history_query = """
                            SELECT field_name, old_value, new_value, modified_at, u.full_name
                            FROM forecast_history fh
                            JOIN users u ON fh.modified_by = u.id
                            WHERE forecast_id = ?
                            ORDER BY modified_at DESC
                        """
                        
                        history_df = pd.read_sql(history_query, conn, params=[sel["id"]])
                        conn.close()
                        
                        if not history_df.empty:
                            for _, hist_row in history_df.iterrows():
                                date_str = pd.to_datetime(hist_row['modified_at']).strftime("%Y-%m-%d %H:%M")
                                st.markdown(f"""
                **{date_str}** par **{hist_row['full_name']}**:  
                Champ: {hist_row['field_name']}  
                Valeur: {hist_row['old_value']} → {hist_row['new_value']}
                ---
                """)
                        else:
                            st.info("Aucun historique disponible pour cette ligne.")
                    except Exception as e:
                        st.error(f"❌ Erreur lors de la récupération de l'historique: {str(e)}")

                         # Commentaire interne
            current_comment = sel.get("internal_comment", "")
            new_comment = st.text_area("🗒️ Commentaire interne (non visible dans le tableau)", value=current_comment, key="internal_comment")
            if st.button("📏 Sauvegarder le commentaire interne"):
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute("UPDATE forecasts SET internal_comment = ? WHERE id = ?", (new_comment, sel["id"]))
                    conn.commit()
                    conn.close()
                    st.success("✅ Commentaire interne mis à jour.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur lors de la mise à jour du commentaire : {e}")
        except Exception as e:
            st.error(f"❌ Erreur lors de l'affichage des détails : {str(e)}")



        # --- 12. EXPORT DES DONNÉES ---
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📄 Exporter en CSV", use_container_width=True):
            # Export CSV simple
            csv = updated_df.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="previsions.csv">Télécharger le fichier CSV</a>'
            st.markdown(href, unsafe_allow_html=True)
    
    with col2:
        if st.button("📊 Exporter en Excel", use_container_width=True):
            # Supprimer les colonnes de tooltip avant l'export
            export_df = updated_df.drop(columns=["tooltip_info", "advanced_tooltip"], errors="ignore")
            
            # Utiliser la nouvelle fonction pour générer un rapport Excel complet
            xlsx_bytes = generate_collab_report(export_df)
            
            # Téléchargement du fichier
            st.download_button(
                label="📥 Télécharger le rapport Excel complet",
                data=xlsx_bytes,
                file_name=f"rapport_collaboratif_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    return updated_df, selected









def preview_sales_df():
    """
    Affiche le preview du DataFrame historique sous le tableau collaboratif.
    """
    # Nombre de lignes (facultatif)
    conn = sqlite3.connect(DB_PATH)
    count = pd.read_sql("SELECT COUNT(*) as cnt FROM sales", conn).iloc[0]["cnt"]
    conn.close()
    st.markdown(f"ℹ️ Nombre de lignes chargées : **{count}**")
    
    # Affichage du DataFrame
    df = pd.read_sql("SELECT * FROM sales LIMIT 5", sqlite3.connect(DB_PATH))
    st.dataframe(df, use_container_width=True)

    


def render_app():
    # 📥 Sidebar Excel import
    with st.sidebar.expander("📥 Importer un fichier Excel", expanded=True):
        # Bouton pour réinitialiser la base de données
        if st.button("🗑️ Réinitialiser la base de données"):
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.execute("DROP TABLE IF EXISTS forecasts")
                conn.execute("DROP TABLE IF EXISTS forecast_history")
                conn.commit()
                conn.close()
                
                # Vider le cache
                if hasattr(get_forecasts, 'clear'):
                    get_forecasts.clear()
                st.cache_data.clear()
                
                st.success("✅ Base de données réinitialisée avec succès!")
                st.info("Vous pouvez maintenant importer un nouveau fichier.")
                time.sleep(1)  # Pause pour éviter les rechargements en boucle
                st.rerun()
            except Exception as e:
                st.error(f"Erreur lors de la réinitialisation : {str(e)}")
        
        st.markdown("---")
        
        excel_file = st.file_uploader(
            "Choisissez un fichier Excel à importer",
            type=["xls", "xlsx"]
        )
        if excel_file is not None:
            import_excel_to_db(excel_file)

    # Initialise la base de données si besoin
    init_database()

    # Ajoute la colonne interne si nécessaire
    ensure_internal_comment_column()

    # Ajout de Font Awesome & Bootstrap Icons
    st.markdown("""
    <!-- Font Awesome 6 -->
    <link rel="stylesheet"
          href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"
          integrity="sha512-iecdLmaskl7CVkqkXNQ/ZH/XLlvWZOJyj7Yy7tcenmpD1ypASozpmT/E0iPtmFIB46ZmdtAc9eNBvH0H/ZpiBw=="
          crossorigin="anonymous" referrerpolicy="no-referrer" />
    <!-- Bootstrap Icons -->
    <link rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">
    """, unsafe_allow_html=True)

    # Navigation principale
    roles = st.session_state.user["role"]
    options = ["Collaboration", "Analyse", "Clients"] + (["Administration"] if roles == "admin" else [])
    icons   = ["pencil-square", "bar-chart-line-fill", "people-fill"] + (["gear-fill"] if roles == "admin" else [])

    choice = option_menu(
        menu_title=None,
        options=options,
        icons=icons,
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "#fafafa"},
            "icon": {"color": COLORS["primary"], "font-size": "1.2rem"},
            "nav-link": {"font-size": "1rem", "text-align": "center", "--hover-color": "#eee"},
            "nav-link-selected": {"background-color": COLORS["primary"], "color": "white"},
        }
    )

    # Route vers les onglets
    if choice == "Collaboration":
        render_fancy_header(
            "Prévisions – Collaboration",
            "Saisie, édition et historique des prévisions",
            "🧠"
        )
        render_collaborative_table()

    elif choice == "Analyse":
        render_fancy_header(
            "Prévisions – Analyse & Export",
            "Analyse visuelle et prévisions automatiques",
            "📈"
        )
        render_forecast_analysis_tab()

    elif choice == "Clients":
        header = "Gestion des Clients" if roles == "admin" else "Mes Clients"
        subtitle = (
            "Attribution et gestion des clients par vendeur"
            if roles == "admin" else
            "Consultation et gestion de vos clients"
        )
        render_fancy_header(header, subtitle, "👥")
        
        # Utiliser render_clients_tab pour les admins et render_client_management_tab pour les utilisateurs
        if roles == "admin":
            render_clients_tab()
        else:
            render_client_management_tab()

    elif choice == "Administration" and roles == "admin":
        render_fancy_header(
            "Administration",
            "Gestion des utilisateurs et paramètres",
            "⚙️"
        )
        render_admin_section()

    else:
        st.error("Accès non autorisé")
        if st.button("← Retour"):
            # on remet par défaut sur Collaboration
            st.rerun()









def render_clients_tab():
    """Affiche l'onglet de gestion des clients"""
    st.markdown("### 👥 Gestion des Clients")

    # Différenciation admin/vendeur
    is_admin = st.session_state.user["role"] == "admin"
    
    if is_admin:
        # Vue administrateur
        st.markdown("#### 🔑 Attribution des clients aux vendeurs")
        
        # Sélection du vendeur
        conn = sqlite3.connect(DB_PATH)
        sales_reps = pd.read_sql(
            "SELECT id, username, full_name FROM users WHERE role = 'user'", 
            conn
        )
        
        selected_rep = st.selectbox(
            "👤 Sélectionner un vendeur",
            options=sales_reps['id'].tolist(),
            format_func=lambda x: sales_reps[sales_reps['id'] == x]['full_name'].iloc[0]
        )

        # Récupérer tous les clients disponibles dans la base
        all_clients_df = pd.read_sql("""
            SELECT DISTINCT ship_to_key, ship_to_name, ship_to_code, country
            FROM forecasts
            WHERE ship_to_key IS NOT NULL
            ORDER BY ship_to_key
        """, conn)
        
        # Récupérer les clients déjà assignés à ce vendeur
        assigned_clients_df = pd.read_sql("""
            SELECT ship_to_key
            FROM client_assignments
            WHERE sales_rep_id = ?
        """, conn, params=[selected_rep])
        
        # Filtrer pour ne montrer que les clients non assignés
        assigned_keys = assigned_clients_df['ship_to_key'].tolist() if not assigned_clients_df.empty else []
        available_clients = all_clients_df[~all_clients_df['ship_to_key'].isin(assigned_keys)]
        
        # Créer un dictionnaire pour l'affichage
        client_display = {}
        for _, row in available_clients.iterrows():
            key = row['ship_to_key']
            name = row['ship_to_name'] if pd.notna(row['ship_to_name']) else "Sans nom"
            client_display[key] = f"{key} - {name}"

        # Formulaire d'ajout de client avec sélection rapide
        with st.form("add_client_form"):
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # Option 1: Sélection rapide d'un client existant
                st.markdown("##### Option 1: Sélection rapide")
                if not available_clients.empty:
                    selected_client = st.selectbox(
                        "Sélectionner un client existant",
                        options=available_clients['ship_to_key'].tolist(),
                        format_func=lambda x: client_display.get(x, x)
                    )
                    use_existing = st.checkbox("Utiliser ce client", value=True)
                else:
                    st.info("Tous les clients sont déjà assignés à ce vendeur.")
                    use_existing = False
            
            with col2:
                # Option 2: Saisie manuelle
                st.markdown("##### Option 2: Saisie manuelle")
                ship_to_key = st.text_input("🔑 Clé ship to")
                ship_to_code = st.text_input("📝 Code client")
                ship_to_name = st.text_input("🏢 Nom du client")
                ship_to_country = st.selectbox(
                    "🌍 Pays",
                    options=["France", "Germany", "Italy", "Spain"]
                )
            
            submitted = st.form_submit_button("➕ Ajouter le client")
            
            if submitted:
                try:
                    cur = conn.cursor()
                    
                    if use_existing and not available_clients.empty:
                        # Utiliser le client sélectionné
                        client_info = available_clients[available_clients['ship_to_key'] == selected_client].iloc[0]
                        cur.execute("""
                            INSERT INTO client_assignments 
                            (sales_rep_id, ship_to_key, ship_to_code, ship_to_name, ship_to_country)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            selected_rep,
                            client_info['ship_to_key'],
                            client_info['ship_to_code'] if pd.notna(client_info['ship_to_code']) else "",
                            client_info['ship_to_name'] if pd.notna(client_info['ship_to_name']) else "",
                            client_info['country'] if pd.notna(client_info['country']) else ""
                        ))
                    else:
                        # Utiliser les données saisies manuellement
                        cur.execute("""
                            INSERT INTO client_assignments 
                            (sales_rep_id, ship_to_key, ship_to_code, ship_to_name, ship_to_country)
                            VALUES (?, ?, ?, ?, ?)
                        """, (selected_rep, ship_to_key, ship_to_code, ship_to_name, ship_to_country))
                    
                    conn.commit()
                    st.success("✅ Client ajouté avec succès!")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("❌ Ce client est déjà assigné à ce vendeur.")
                except Exception as e:
                    st.error(f"❌ Erreur lors de l'ajout : {str(e)}")

        # Affichage des clients actuels du vendeur
        clients = pd.read_sql("""
            SELECT 
                ca.id,
                ca.ship_to_key,
                ca.ship_to_code,
                ca.ship_to_name,
                ca.ship_to_country,
                ca.created_at
            FROM client_assignments ca
            WHERE ca.sales_rep_id = ?
        """, conn, params=[selected_rep])

        if not clients.empty:
            st.markdown("#### 📋 Clients assignés")
            
            # Ajout d'un bouton de suppression pour chaque client
            def delete_button(row):
                if st.button("🗑️", key=f"delete_{row['id']}"):
                    cur = conn.cursor()
                    cur.execute(
                        "DELETE FROM client_assignments WHERE id = ?",
                        (row['id'],)
                    )
                    conn.commit()
                    st.success("✅ Client supprimé!")
                    st.rerun()
                return ""

            clients['Actions'] = clients.apply(delete_button, axis=1)
            st.dataframe(clients.drop('id', axis=1), use_container_width=True)
        else:
            st.info("ℹ️ Aucun client assigné à ce vendeur.")

    else:
        # Vue vendeur
        st.markdown("#### 📋 Mes Clients")
        
        # Affichage des clients du vendeur connecté
        conn = sqlite3.connect(DB_PATH)
        clients = pd.read_sql("""
            SELECT 
                ship_to_key,
                ship_to_code,
                ship_to_name,
                ship_to_country,
                created_at
            FROM client_assignments
            WHERE sales_rep_id = ?
        """, conn, params=[st.session_state.user["id"]])

        if not clients.empty:
            st.dataframe(clients, use_container_width=True)
        else:
            st.info("ℹ️ Aucun client ne vous est assigné pour le moment.")

    if 'conn' in locals():
        conn.close()


def enrich_forecasts_with_packaging_rules(forecasts_df):
    """
    Enrichit le DataFrame des prévisions avec les règles d'emballage correspondantes.
    
    Args:
        forecasts_df: DataFrame contenant les prévisions
    
    Returns:
        DataFrame enrichi avec les règles d'emballage
    """
    try:
        # Récupérer toutes les règles d'emballage
        packaging_rules_df = get_packaging_rules()
        
        if packaging_rules_df.empty:
            # Si aucune règle n'est définie, utiliser des valeurs par défaut
            forecasts_df['moq_kg'] = 1000  # MOQ par défaut de 1 tonne
            forecasts_df['pallet_kg'] = 1000  # Poids standard d'une palette
            forecasts_df['box_kg'] = 25  # Poids standard d'une boîte
            forecasts_df['boxes_per_pallet'] = 40  # Nombre de boîtes par palette
            return forecasts_df
        
        # Créer un dictionnaire pour un accès rapide aux règles par code produit
        rules_dict = {}
        for _, rule in packaging_rules_df.iterrows():
            key = (rule['site_code'], rule['product_code'])
            rules_dict[key] = {
                'moq_kg': rule['moq_kg'],
                'pallet_kg': rule['pallet_size_kg'],
                'box_kg': rule['packing_size_kg'],
                'boxes_per_pallet': int(rule['pallet_size_kg'] / rule['packing_size_kg']) if rule['packing_size_kg'] > 0 else 0
            }
        
        # Ajouter les colonnes d'emballage au DataFrame des prévisions
        forecasts_df['moq_kg'] = 0
        forecasts_df['pallet_kg'] = 0
        forecasts_df['box_kg'] = 0
        forecasts_df['boxes_per_pallet'] = 0
        
        # Pour chaque ligne de prévision, chercher les règles correspondantes
        for idx, row in forecasts_df.iterrows():
            site_code = row.get('site_code', 'DEFAULT')
            product_code = row.get('material_code', '')
            
            # Essayer d'abord avec le site spécifique
            key = (site_code, product_code)
            if key in rules_dict:
                rule = rules_dict[key]
            else:
                # Essayer avec le site par défaut
                key = ('DEFAULT', product_code)
                rule = rules_dict.get(key, {
                    'moq_kg': 1000,
                    'pallet_kg': 1000,
                    'box_kg': 25,
                    'boxes_per_pallet': 40
                })
            
            # Appliquer les règles
            forecasts_df.at[idx, 'moq_kg'] = rule['moq_kg']
            forecasts_df.at[idx, 'pallet_kg'] = rule['pallet_kg']
            forecasts_df.at[idx, 'box_kg'] = rule['box_kg']
            forecasts_df.at[idx, 'boxes_per_pallet'] = rule['boxes_per_pallet']
        
        return forecasts_df
        
    except Exception as e:
        st.warning(f"⚠️ Impossible d'enrichir les prévisions avec les règles d'emballage: {str(e)}")
        # En cas d'erreur, retourner le DataFrame original
        return forecasts_df




def render_admin_section():
    """
    Affiche la section d'administration avec la gestion des utilisateurs et des règles d'emballage
    """
    # Création des onglets pour les différentes sections d'administration
    tabs = st.tabs(["👥 Utilisateurs", "📦 Règles d'emballage"])
    
    # Onglet Utilisateurs
    with tabs[0]:
        try:
            conn = sqlite3.connect(DB_PATH)
            
            # Affichage des utilisateurs existants
            users_df = pd.read_sql("SELECT id, username, full_name, role FROM users", conn)
            st.markdown("### 👥 Gestion des utilisateurs")
            st.dataframe(users_df)

            # Formulaire d'ajout d'utilisateur
            with st.expander("➕ Ajouter un utilisateur"):
                with st.form("add_user_form"):
                    username = st.text_input("Nom d'utilisateur")
                    password = st.text_input("Mot de passe", type="password")
                    full_name = st.text_input("Nom complet")
                    role = st.selectbox("Rôle", ["user", "admin"])
                    
                    if st.form_submit_button("Ajouter"):
                        if username and password and full_name:
                            try:
                                # Hash du mot de passe
                                password_hash = hashlib.sha256(password.encode()).hexdigest()
                                
                                # Insertion dans la base de données
                                cur = conn.cursor()
                                cur.execute("""
                                    INSERT INTO users (username, password_hash, full_name, role)
                                    VALUES (?, ?, ?, ?)
                                """, (username, password_hash, full_name, role))
                                conn.commit()
                                st.success("✅ Utilisateur ajouté avec succès!")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("❌ Ce nom d'utilisateur existe déjà.")
                            except Exception as e:
                                st.error(f"❌ Erreur lors de l'ajout : {str(e)}")
                        else:
                            st.warning("⚠️ Veuillez remplir tous les champs.")

            # Réinitialisation de mot de passe
            with st.expander("🔑 Réinitialiser un mot de passe"):
                with st.form("reset_password_form"):
                    user_to_reset = st.selectbox(
                        "Sélectionner l'utilisateur",
                        options=users_df["username"].tolist(),
                        key="user_reset"
                    )
                    new_password = st.text_input("Nouveau mot de passe", type="password")
                    confirm_password = st.text_input("Confirmer le mot de passe", type="password")
                    
                    if st.form_submit_button("Réinitialiser le mot de passe"):
                        if new_password and confirm_password:
                            if new_password == confirm_password:
                                try:
                                    # Hash du nouveau mot de passe
                                    new_password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                                    
                                    # Mise à jour dans la base de données
                                    cur = conn.cursor()
                                    cur.execute("""
                                        UPDATE users 
                                        SET password_hash = ?,
                                            remember_token = NULL,
                                            token_expiry = NULL
                                        WHERE username = ?
                                    """, (new_password_hash, user_to_reset))
                                    conn.commit()
                                    st.success("✅ Mot de passe réinitialisé avec succès!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Erreur lors de la réinitialisation : {str(e)}")
                            else:
                                st.error("❌ Les mots de passe ne correspondent pas.")
                        else:
                            st.warning("⚠️ Veuillez remplir tous les champs.")

            # Suppression d'utilisateur
            with st.expander("🗑️ Supprimer un utilisateur"):
                user_to_delete = st.selectbox(
                    "Sélectionner l'utilisateur à supprimer",
                    options=users_df["username"].tolist(),
                    key="user_delete"
                )
                if st.button("Supprimer", key="delete_button"):
                    if user_to_delete != "admin":  # Protection de l'admin
                        try:
                            cur = conn.cursor()
                            cur.execute("DELETE FROM users WHERE username = ?", (user_to_delete,))
                            conn.commit()
                            st.success("✅ Utilisateur supprimé avec succès!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Erreur lors de la suppression : {str(e)}")
                    else:
                        st.error("❌ Impossible de supprimer l'administrateur principal.")

        except Exception as e:
            st.error(f"❌ Erreur dans la section administration : {str(e)}")
        
        finally:
            if 'conn' in locals():
                conn.close()
    
    # Onglet Règles d'emballage
    with tabs[1]:
        st.markdown("### 📦 Gestion des règles d'emballage")
        
        # Afficher les statistiques actuelles
        try:
            conn = sqlite3.connect(DB_PATH)
            rule_count = pd.read_sql("SELECT COUNT(*) as count FROM packaging_rules", conn).iloc[0]['count']
            last_import = pd.read_sql("""
                SELECT import_date, file_name, record_count 
                FROM packaging_rules_versions 
                ORDER BY import_date DESC LIMIT 1
            """, conn)
            conn.close()
            
            if not last_import.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Règles d'emballage", f"{rule_count}")
                with col2:
                    st.metric("Dernier import", f"{last_import.iloc[0]['import_date']}")
                with col3:
                    st.metric("Fichier", f"{last_import.iloc[0]['file_name']}")
        except Exception as e:
            st.error(f"❌ Erreur lors de la récupération des statistiques: {str(e)}")
        
        # Interface d'import - Format standard
        with st.expander("📤 Importer un fichier au format standard", expanded=False):
            st.info("Format standard: un onglet 'PackagingRules' avec les colonnes site_code, product_code, etc.")
            uploaded_file = st.file_uploader(
                "Sélectionnez le fichier Excel des règles d'emballage (format standard)",
                type=["xlsx", "xls"],
                key="packaging_rules_file_standard"
            )
            
            if uploaded_file is not None:
                # Afficher un aperçu du fichier
                try:
                    preview_df = pd.read_excel(uploaded_file, sheet_name="PackagingRules", nrows=5)
                    st.write("Aperçu du fichier:")
                    st.dataframe(preview_df)
                    
                    # Bouton d'import
                    if st.button("🚀 Importer les règles d'emballage (format standard)"):
                        uploaded_file.seek(0)  # Réinitialiser le pointeur de fichier
                        success = import_packaging_rules(uploaded_file, st.session_state.user["id"])
                        if success:
                            st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur lors de la lecture du fichier: {str(e)}")
        
        # Interface d'import - Format transposé (nouveau)
        with st.expander("📤 Importer un fichier au format transposé", expanded=True):
            st.info("Format transposé: produits en colonnes, caractéristiques en lignes (Packing, Pallet size, MOQ, MRQ)")
            uploaded_file_transposed = st.file_uploader(
                "Sélectionnez le fichier Excel des règles d'emballage (format transposé)",
                type=["xlsx", "xls"],
                key="packaging_rules_file_transposed"
            )
            
            if uploaded_file_transposed is not None:
                # Afficher un aperçu du fichier
                try:
                    preview_df = pd.read_excel(uploaded_file_transposed, sheet_name="Feuil1", header=None, nrows=5)
                    st.write("Aperçu du fichier:")
                    st.dataframe(preview_df)
                    
                    # Bouton d'import
                    if st.button("🚀 Importer les règles d'emballage (format transposé)"):
                        result = import_packaging_rules_from_excel(uploaded_file_transposed)
                        st.success(f"✅ Import réussi: {result['inserted']} règles ajoutées, {result['updated']} règles mises à jour")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur lors de la lecture du fichier: {str(e)}")
        
        # Afficher les règles actuelles
        with st.expander("📋 Règles d'emballage actuelles", expanded=False):
            rules_df = get_packaging_rules()
            if not rules_df.empty:
                st.dataframe(rules_df, use_container_width=True)
                
                # Export des règles
                if st.button("📥 Exporter les règles"):
                    csv = rules_df.to_csv(index=False)
                    b64 = base64.b64encode(csv.encode()).decode()
                    href = f'<a href="data:file/csv;base64,{b64}" download="regles_emballage.csv">Télécharger le fichier CSV</a>'
                    st.markdown(href, unsafe_allow_html=True)
            else:
                st.info("Aucune règle d'emballage n'est définie.")



def main():
    """
    Fonction principale de l'application (sans splash screen)
    """
    try:
        # Chargement des variables d'environnement et initialisation
        load_dotenv()
        init_database()

        # Ajout de la colonne commentaire interne si elle n'existe pas
        ensure_internal_comment_column()

        # Initialisation des états de session
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'user' not in st.session_state:
            st.session_state.user = None
        if 'remember_me' not in st.session_state:
            st.session_state.remember_me = False
        if 'page' not in st.session_state:
            st.session_state.page = "Dashboard"

        # Vérification du token "remember me"
        if not st.session_state.authenticated and st.session_state.remember_me:
            user = check_remember_token(st.session_state.get('remember_token'))
            if user:
                st.session_state.authenticated = True
                st.session_state.user = user
                st.success(f"Bienvenue {user['full_name']} !")

        # Affichage de l'application
        if not st.session_state.authenticated:
            render_login()
        else:
            render_app()

    except Exception as e:
        st.error(f"Une erreur est survenue : {str(e)}")
        st.error("Veuillez rafraîchir la page ou contacter l'administrateur.")


if __name__ == "__main__":
    main()
