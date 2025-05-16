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
        if st.button("📥 Exporter les données"):
            csv = updated_df.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="previsions.csv">Télécharger le fichier CSV</a>'
            st.markdown(href, unsafe_allow_html=True)
    
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
    col1, col2, col3 = st.columns(3)
    with col1:
        product_filter = st.multiselect(
            "🧪 Filtrer par produit",
            options=sorted([p for p in original_df['product_line'].unique() if p is not None]),
            default=[]
        )

    with col2:
        country_filter = st.multiselect(
            "🌍 Filtrer par pays",
            options=sorted([c for c in original_df['country'].unique() if c is not None]),
            default=[]
        )
    with col3:
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

    filtered_df = original_df.copy()
    if product_filter:
        filtered_df = filtered_df[filtered_df['product_line'].isin(product_filter)]
    if country_filter:
        filtered_df = filtered_df[filtered_df['country'].isin(country_filter)]
    if 'client_type' in original_df.columns and client_filter:
        filtered_df = filtered_df[filtered_df['client_type'].isin(client_filter)]

    # ... suite de la construction de df_display et configuration d’AgGrid ...


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

    # 1) Colonnes toujours visibles
    default_display_cols = [
        "ship_to_key",
        "ship_to_name",
        "material_description"
    ]

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

    # 3) Colonnes full-year (toujours à la fin)
    for col in ["full_year_budget", "full_year_forecast"]:
        if col in filtered_df.columns:
            default_display_cols.append(col)

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
            # Historique des 3 derniers mois - avec gestion des erreurs
            history = []
            try:
                if 'ship_to_key' in row and 'material_description' in row:
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
                    
                    history_df = pd.read_sql(history_query, conn, params=[row["ship_to_key"]])
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
                orderbook_data = row.get("orderbook", {})
                if isinstance(orderbook_data, dict):
                    orders = {k: v for k, v in orderbook_data.items() if isinstance(v, (int, float)) and v > 0}
            except Exception:
                pass
            
            # Budget mensuel (budget_dd)
            budget = {}
            try:
                budget_data = row.get("budget_dd", {})
                if isinstance(budget_data, dict):
                    budget = {k: v for k, v in budget_data.items() if isinstance(v, (int, float))}
            except Exception:
                pass
            
            # Historique des variations de backlog
            backlog = {}
            try:
                backlog_data = row.get("backlog_variation", {})
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
            let content = '<div style="background-color: white; border: 1px solid #ddd; padding: 15px; border-radius: 8px; box-shadow: 0 3px 10px rgba(0,0,0,0.2); max-width: 350px;">';
            
            // Titre
            const shipToKey = data.ship_to_key || 'ID inconnu';
            const materialDesc = data.material_description || 'Produit inconnu';
            content += `<h4 style="margin-top: 0; color: #1f77b4; border-bottom: 1px solid #eee; padding-bottom: 8px;">${shipToKey} - ${materialDesc}</h4>`;
            
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
                content += '<ul style="margin-top: 0; padding-left: 20px;">';
                Object.entries(tooltipData.budget).forEach(([month, value]) => {
                    content += `<li>${month}: <b>${value}</b></li>`;
                });
                content += '</ul>';
            } else {
                content += '<p style="margin: 0; color: #777;">Aucun budget disponible</p>';
            }
            
            // Variations
            content += '<h5 style="margin-bottom: 5px; margin-top: 15px; color: #555;">📊 Variations</h5>';
            if (tooltipData.backlog && Object.keys(tooltipData.backlog).length > 0) {
                content += '<ul style="margin-top: 0; padding-left: 20px;">';
                Object.entries(tooltipData.backlog).forEach(([month, value]) => {
                    content += `<li>${month}: <b>${value}</b></li>`;
                });
                content += '</ul>';
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
            
            // Fermer le tooltip au clic n'importe où
            document.addEventListener('click', function closeTooltip() {
                tooltip.remove();
                document.removeEventListener('click', closeTooltip);
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
    if st.button("💾 Exporter les données"):
        # Création d'un fichier Excel en mémoire
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Supprimer les colonnes de tooltip avant l'export
            export_df = updated_df.drop(columns=["tooltip_info", "advanced_tooltip"], errors="ignore")
            export_df.to_excel(writer, index=False, sheet_name='Prévisions')
        
        # Téléchargement du fichier
        st.download_button(
            label="📥 Télécharger le fichier Excel",
            data=output.getvalue(),
            file_name=f"previsions_modifiees_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
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
        render_clients_tab()

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

        # Formulaire d'ajout de client
        with st.form("add_client_form"):
            col1, col2 = st.columns(2)
            with col1:
                ship_to_key = st.text_input("🔑 Clé ship to")
                ship_to_code = st.text_input("📝 Code client")
            with col2:
                ship_to_name = st.text_input("🏢 Nom du client")
                ship_to_country = st.selectbox(
                    "🌍 Pays",
                    options=["France", "Germany", "Italy", "Spain"]
                )
            
            submitted = st.form_submit_button("➕ Ajouter le client")
            
            if submitted:
                try:
                    cur = conn.cursor()
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


def render_admin_section():
    """
    Affiche la section d'administration avec la gestion des utilisateurs
    """
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