import pandas as pd
import sqlite3

def load_sales_history(db_path: str, excel_path: str):
    """
    Charge l'historique des ventes depuis un fichier Excel dans la table 'sales' de SQLite.
    """

    # 1) Lire le fichier Excel
    df = pd.read_excel(excel_path, engine='openpyxl')

    # 2) Nettoyer les noms de colonnes (trim)
    df.columns = df.columns.str.strip()

    # 3) Renommer automatiquement les doublons de colonnes
    def rename_duplicates(columns):
        seen = {}
        new_cols = []
        for col in columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        return new_cols

    df.columns = rename_duplicates(df.columns)

    # 4) Renommer manuellement les doublons critiques
    #    (ex : zone → zone_main / zone_alt)
    zone_idxs = [i for i, c in enumerate(df.columns) if c == "zone"]
    if len(zone_idxs) >= 2:
        df.columns.values[zone_idxs[0]] = "zone_main"
        df.columns.values[zone_idxs[1]] = "zone_alt"

    #    (ex : country → country / country_alt)
    country_idxs = [i for i, c in enumerate(df.columns) if c == "country"]
    if len(country_idxs) >= 2:
        df.columns.values[country_idxs[0]] = "country"
        df.columns.values[country_idxs[1]] = "country_alt"

    # 5) Parser les dates si présentes
    for date_col in ["Order Date", "Good issue"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # 6) Renommer les colonnes pour matcher la table SQL
    rename_map = {
        "Year": "year",
        "Period": "period",
        "Week": "week",
        "Sales document": "sales_document",
        "Order Date": "order_date",
        "Good issue": "delivery_date",
        "Invoiced": "invoiced",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 7) Créer un flag binaire pour invoiced
    if "invoiced" in df.columns:
        df["invoiced_flag"] = (
            df["invoiced"]
            .astype(str)
            .str.strip()
            .str.upper()
            .eq("YES")
            .astype(int)
        )

    # 8) Écrire dans SQLite (remplace la table si elle existe)
    conn = sqlite3.connect(db_path)
    df.to_sql("sales", conn, if_exists="replace", index=False)
    conn.close()
