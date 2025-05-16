import pandas as pd
import numpy as np
from prophet import Prophet
import traceback
from datetime import datetime
import statsmodels.api as sm
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

def train_prophet_model(df, product_column, country_column, date_column, value_column, data_type_column, forecast_periods=12):
    """
    Version améliorée avec ensemble de modèles pour maximiser la fiabilité.
    """
    required_columns = [product_column, country_column, date_column, value_column, data_type_column]
    for col in required_columns:
        if col not in df.columns:
            print(f"Colonne manquante: {col}")
            return {}
    
    models = {}
    
    try:
        # Identifier les combinaisons uniques produit-pays
        if product_column in df.columns and country_column in df.columns:
            unique_products = df[product_column].unique()
            unique_countries = df[country_column].unique()
        else:
            # Si les colonnes n'existent pas, utiliser des valeurs par défaut
            unique_products = ['Tous produits']
            unique_countries = ['Tous pays']
            df[product_column] = 'Tous produits'
            df[country_column] = 'Tous pays'
        
        for product in unique_products:
            for country in unique_countries:
                try:
                    # Filtrer les données pour ce produit et ce pays
                    subset = df[
                        (df[product_column] == product) &
                        (df[country_column] == country)
                    ].copy()
                    
                    # 1. AMÉLIORATION: Détection et traitement des valeurs aberrantes
                    if len(subset) >= 10:  # Si assez de données pour calculer des statistiques
                        Q1 = subset[value_column].quantile(0.25)
                        Q3 = subset[value_column].quantile(0.75)
                        IQR = Q3 - Q1
                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR
                        
                        # Remplacer les valeurs aberrantes par les bornes
                        subset.loc[subset[value_column] < lower_bound, value_column] = lower_bound
                        subset.loc[subset[value_column] > upper_bound, value_column] = upper_bound
                    
                    # Vérifier si nous avons assez de données
                    if len(subset) < 5:
                        continue
                    
                    # 2. AMÉLIORATION: Préparer les données avec plus de soin
                    prophet_df = subset[[date_column, value_column]].rename(
                        columns={date_column: 'ds', value_column: 'y'}
                    )
                    
                    # Trier par date pour s'assurer que la séquence temporelle est correcte
                    prophet_df = prophet_df.sort_values('ds')
                    
                    # Supprimer les doublons et les valeurs manquantes
                    prophet_df = prophet_df.drop_duplicates(subset=['ds'])
                    prophet_df = prophet_df.dropna()
                    
                    # Si nous n'avons pas assez de données après nettoyage, passer
                    if len(prophet_df) < 5:
                        continue
                    
                    # 3. Préparer les données pour l'ensemble
                    train_data = prophet_df.copy()
                    
                    # Caractéristiques temporelles pour les modèles ML
                    train_data['month'] = train_data['ds'].dt.month
                    train_data['quarter'] = train_data['ds'].dt.quarter
                    train_data['year'] = train_data['ds'].dt.year
                    
                    # 1.1 Modèle Prophet amélioré
                    prophet_model = fit_prophet_model(train_data, forecast_periods)
                    
                    # 1.2 Modèle SARIMA (pour séries temporelles avec saisonnalité)
                    sarima_forecast = None
                    if len(train_data) >= 12:  # SARIMA a besoin d'au moins un an de données
                        sarima_forecast = fit_sarima_model(train_data, forecast_periods)
                    
                    # 1.3 Modèle RandomForest
                    rf_forecast = fit_randomforest_model(train_data, forecast_periods)
                    
                    # 1.4 Modèle XGBoost
                    xgb_forecast = fit_xgboost_model(train_data, forecast_periods)
                    
                    # 2. COMBINAISON DES PRÉVISIONS
                    if prophet_model is not None:
                        forecast_dates = prophet_model['forecast']['ds']
                        ensemble_forecast = pd.DataFrame({'ds': forecast_dates})
                        
                        # Ajouter les prévisions de chaque modèle
                        if prophet_model is not None:
                            ensemble_forecast['prophet'] = prophet_model['forecast']['yhat']
                        else:
                            ensemble_forecast['prophet'] = np.nan
                        
                        if sarima_forecast is not None:
                            ensemble_forecast = pd.merge(
                                ensemble_forecast,
                                sarima_forecast[['ds', 'yhat']].rename(columns={'yhat': 'sarima'}),
                                on='ds', how='left'
                            )
                        else:
                            ensemble_forecast['sarima'] = np.nan
                        
                        if rf_forecast is not None:
                            ensemble_forecast = pd.merge(
                                ensemble_forecast,
                                rf_forecast[['ds', 'yhat']].rename(columns={'yhat': 'randomforest'}),
                                on='ds', how='left'
                            )
                        else:
                            ensemble_forecast['randomforest'] = np.nan
                        
                        if xgb_forecast is not None:
                            ensemble_forecast = pd.merge(
                                ensemble_forecast,
                                xgb_forecast[['ds', 'yhat']].rename(columns={'yhat': 'xgboost'}),
                                on='ds', how='left'
                            )
                        else:
                            ensemble_forecast['xgboost'] = np.nan
                        
                        # Déterminer les poids en fonction de la performance
                        weights = determine_model_weights(train_data, ensemble_forecast)
                        
                        # Calculer la prévision finale comme moyenne pondérée
                        ensemble_forecast['yhat'] = 0
                        for model, weight in weights.items():
                            ensemble_forecast[model] = ensemble_forecast[model].fillna(0)
                            if weight > 0:
                                ensemble_forecast['yhat'] += weight * ensemble_forecast[model]
                        
                        # S'assurer que les prévisions sont positives
                        ensemble_forecast['yhat'] = ensemble_forecast['yhat'].clip(lower=0)
                        
                        # Calculer les intervalles de confiance
                        if prophet_model is not None:
                            ensemble_forecast['yhat_lower'] = prophet_model['forecast']['yhat_lower']
                            ensemble_forecast['yhat_upper'] = prophet_model['forecast']['yhat_upper']
                            
                            # Ajuster les intervalles pour l'incertitude
                            uncertainty_factor = 1.2
                            ensemble_forecast['yhat_lower'] = ensemble_forecast['yhat'] - uncertainty_factor * (ensemble_forecast['yhat'] - ensemble_forecast['yhat_lower'])
                            ensemble_forecast['yhat_upper'] = ensemble_forecast['yhat'] + uncertainty_factor * (ensemble_forecast['yhat_upper'] - ensemble_forecast['yhat'])
                            
                            # S'assurer que les bornes inférieures sont positives
                            ensemble_forecast['yhat_lower'] = ensemble_forecast['yhat_lower'].clip(lower=0)
                        else:
                            # Intervalles par défaut
                            std_dev = train_data['y'].std()
                            ensemble_forecast['yhat_lower'] = ensemble_forecast['yhat'] * 0.8
                            ensemble_forecast['yhat_upper'] = ensemble_forecast['yhat'] * 1.2
                        
                        # Arrondir les valeurs
                        ensemble_forecast['yhat'] = ensemble_forecast['yhat'].round(0).astype(int)
                        ensemble_forecast['yhat_lower'] = ensemble_forecast['yhat_lower'].round(0).astype(int)
                        ensemble_forecast['yhat_upper'] = ensemble_forecast['yhat_upper'].round(0).astype(int)
                        
                        # Créer structure similaire à Prophet
                        key = f"{product} - {country}"
                        
                        if prophet_model is not None:
                            prophet_forecast = prophet_model['forecast'].copy()
                            prophet_forecast['yhat'] = ensemble_forecast['yhat'].values
                            prophet_forecast['yhat_lower'] = ensemble_forecast['yhat_lower'].values
                            prophet_forecast['yhat_upper'] = ensemble_forecast['yhat_upper'].values
                            
                            models[key] = {
                                'model': prophet_model['model'],
                                'forecast': prophet_forecast,
                                'product': product,
                                'country': country,
                                'training_data': train_data[['ds', 'y']],
                                'model_weights': weights
                            }
                        else:
                            # Structure compatible sans Prophet
                            models[key] = {
                                'model': None,
                                'forecast': ensemble_forecast,
                                'product': product,
                                'country': country,
                                'training_data': train_data[['ds', 'y']],
                                'model_weights': weights
                            }
                    
                except Exception as e:
                    print(f"Erreur lors de l'entraînement du modèle pour {product} - {country}: {str(e)}")
                    traceback.print_exc()
        
        return models
    
    except Exception as e:
        print(f"Erreur lors de l'entraînement des modèles: {str(e)}")
        traceback.print_exc()
        return {}

def determine_model_weights(train_data, ensemble_forecast):
    """
    Détermine les poids de chaque modèle en fonction de leurs performances.
    """
    try:
        # Obtenir les dates d'entraînement
        train_dates = train_data['ds']
        
        # Filtrer les prévisions pour les dates d'entraînement
        historical_forecast = ensemble_forecast[ensemble_forecast['ds'].isin(train_dates)].copy()
        
        # Fusionner avec les valeurs réelles
        evaluation = pd.merge(historical_forecast, train_data[['ds', 'y']], on='ds', how='inner')
        
        # Poids par défaut
        weights = {
            'prophet': 0.25,
            'sarima': 0.25,
            'randomforest': 0.25,
            'xgboost': 0.25
        }
        
        # Si assez de données pour évaluer
        if len(evaluation) > 0:
            errors = {}
            
            # Calculer l'erreur pour chaque modèle
            for model in ['prophet', 'sarima', 'randomforest', 'xgboost']:
                if model in evaluation.columns and not evaluation[model].isna().all():
                    errors[model] = mean_absolute_error(evaluation['y'], evaluation[model])
                else:
                    errors[model] = float('inf')
            
            # Calculer les poids inversement proportionnels aux erreurs
            total_error = sum(1/e if e > 0 else 0 for e in errors.values())
            
            if total_error > 0:
                for model, error in errors.items():
                    if error > 0:
                        weights[model] = (1/error) / total_error
                    else:
                        weights[model] = 0
        
        # Normaliser les poids
        total_weight = sum(weights.values())
        if total_weight > 0:
            for model in weights:
                weights[model] = weights[model] / total_weight
        else:
            # Si aucun modèle évaluable, utiliser des poids égaux
            valid_models = [m for m, w in weights.items() if w > 0]
            for model in valid_models:
                weights[model] = 1.0 / len(valid_models) if len(valid_models) > 0 else 0
        
        return weights
    
    except Exception as e:
        print(f"Erreur lors de la détermination des poids: {str(e)}")
        # Poids par défaut
        return {
            'prophet': 0.4,
            'sarima': 0.2,
            'randomforest': 0.2,
            'xgboost': 0.2
        }

def fit_prophet_model(train_data, forecast_periods):
    """
    Entraîne un modèle Prophet amélioré.
    """
    try:
        # Créer une copie pour ne pas modifier l'original
        prophet_df = train_data[['ds', 'y']].copy()
        
        # Détecter le mode de saisonnalité
        min_val = prophet_df['y'].min()
        max_val = prophet_df['y'].max()
        seasonality_mode = 'multiplicative' if max_val / (min_val + 1e-10) > 3 else 'additive'
        
        # Ajuster les paramètres en fonction de la quantité de données
        n_changepoints = min(25, max(5, int(len(prophet_df) / 10)))
        
        # Créer les jours fériés
        years = list(range(
            prophet_df['ds'].dt.year.min() - 1,
            prophet_df['ds'].dt.year.max() + 2
        ))
        
        holidays = pd.DataFrame({
            'holiday': 'new_year',
            'ds': pd.to_datetime([f'{year}-01-01' for year in years]),
            'lower_window': -1,
            'upper_window': 1,
        })
        
        # Ajouter Noël
        christmas = pd.DataFrame({
            'holiday': 'christmas',
            'ds': pd.to_datetime([f'{year}-12-25' for year in years]),
            'lower_window': -7,
            'upper_window': 3,
        })
        holidays = pd.concat([holidays, christmas])
        
        # Créer et entraîner le modèle
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False if len(prophet_df) < 15 else True,
            daily_seasonality=False,
            seasonality_mode=seasonality_mode,
            interval_width=0.95,
            changepoint_prior_scale=0.05,
            n_changepoints=n_changepoints,
            holidays=holidays
        )
        
        # Ajouter une saisonnalité mensuelle explicite
        model.add_seasonality(
            name='monthly', 
            period=30.5, 
            fourier_order=5
        )
        
        model.fit(prophet_df)
        
        # Générer les prévisions
        future = model.make_future_dataframe(periods=forecast_periods, freq='MS')
        forecast = model.predict(future)
        
        return {
            'model': model,
            'forecast': forecast
        }
    
    except Exception as e:
        print(f"Erreur lors de l'entraînement du modèle Prophet: {str(e)}")
        return None

def fit_sarima_model(train_data, forecast_periods):
    """
    Entraîne un modèle SARIMA pour les séries temporelles saisonnières.
    """
    try:
        # Créer une série temporelle indexée par date
        ts = train_data.set_index('ds')['y']
        
        # Si assez de données pour un modèle saisonnier
        if len(ts) >= 12:
            # Paramètres SARIMA simplifiés
            order = (1, 1, 1)
            seasonal_order = (1, 0, 1, 12)
            
            # Entraîner le modèle
            model = SARIMAX(
                ts,
                order=order,
                seasonal_order=seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            results = model.fit(disp=False)
            
            # Générer les prévisions
            forecast_index = pd.date_range(
                start=ts.index[-1] + pd.DateOffset(months=1),
                periods=forecast_periods,
                freq='MS'
            )
            sarima_forecast = results.forecast(steps=forecast_periods)
            
            # Créer un DataFrame de prévisions
            forecast_df = pd.DataFrame({
                'ds': pd.to_datetime(forecast_index),
                'yhat': sarima_forecast.values
            })
            
            # Ajouter les données historiques
            historical = pd.DataFrame({
                'ds': train_data['ds'],
                'yhat': results.fittedvalues.values
            })
            
            forecast_df = pd.concat([historical, forecast_df])
            
            # Assurer que les prévisions sont positives
            forecast_df['yhat'] = forecast_df['yhat'].clip(lower=0)
            
            return forecast_df
        
        return None
    
    except Exception as e:
        print(f"Erreur lors de l'entraînement du modèle SARIMA: {str(e)}")
        return None

def fit_randomforest_model(train_data, forecast_periods):
    """
    Entraîne un modèle RandomForest pour les prévisions.
    """
    try:
        # Créer des caractéristiques
        X = train_data[['month', 'quarter', 'year']].copy()
        y = train_data['y'].values
        
        # Entraîner le modèle
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=2,
            min_samples_leaf=1,
            random_state=42
        )
        model.fit(X, y)
        
        # Générer les caractéristiques pour les prévisions futures
        future_dates = []
        last_date = train_data['ds'].max()
        
        for i in range(1, forecast_periods + 1):
            next_date = pd.Timestamp(last_date) + pd.DateOffset(months=i)
            future_dates.append(next_date)
        
        future_df = pd.DataFrame({'ds': future_dates})
        future_df['month'] = future_df['ds'].dt.month
        future_df['quarter'] = future_df['ds'].dt.quarter
        future_df['year'] = future_df['ds'].dt.year
        
        # Prévisions
        future_X = future_df[['month', 'quarter', 'year']]
        future_df['yhat'] = model.predict(future_X)
        
        # Prévisions sur les données d'entraînement
        train_X = train_data[['month', 'quarter', 'year']]
        train_preds = pd.DataFrame({
            'ds': train_data['ds'],
            'yhat': model.predict(train_X)
        })
        
        # Combiner les prévisions historiques et futures
        all_preds = pd.concat([train_preds, future_df[['ds', 'yhat']]])
        
        # Assurer que les prévisions sont positives
        all_preds['yhat'] = all_preds['yhat'].clip(lower=0)
        
        return all_preds
    
    except Exception as e:
        print(f"Erreur lors de l'entraînement du modèle RandomForest: {str(e)}")
        return None

def fit_xgboost_model(train_data, forecast_periods):
    """
    Entraîne un modèle XGBoost pour les prévisions.
    """
    try:
        # Créer des caractéristiques
        X = train_data[['month', 'quarter', 'year']].copy()
        y = train_data['y'].values
        
        # Entraîner le modèle
        model = XGBRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42
        )
        model.fit(X, y)
        
        # Générer les caractéristiques pour les prévisions futures
        future_dates = []
        last_date = train_data['ds'].max()
        
        for i in range(1, forecast_periods + 1):
            next_date = pd.Timestamp(last_date) + pd.DateOffset(months=i)
            future_dates.append(next_date)
        
        future_df = pd.DataFrame({'ds': future_dates})
        future_df['month'] = future_df['ds'].dt.month
        future_df['quarter'] = future_df['ds'].dt.quarter
        future_df['year'] = future_df['ds'].dt.year
        
        # Prévisions sans variables décalées
        future_X = future_df[['month', 'quarter', 'year']]
        future_df['yhat'] = model.predict(future_X)
        
        # Prévisions sur les données d'entraînement
        train_X = train_data[['month', 'quarter', 'year']]
        train_preds = pd.DataFrame({
            'ds': train_data['ds'],
            'yhat': model.predict(train_X)
        })
        
        # Combiner les prévisions
        all_preds_df = pd.concat([train_preds, future_df[['ds', 'yhat']]])
        
        # Assurer que les prévisions sont positives
        all_preds_df['yhat'] = all_preds_df['yhat'].clip(lower=0)
        
        return all_preds_df
    
    except Exception as e:
        print(f"Erreur lors de l'entraînement du modèle XGBoost: {str(e)}")
        return None

def get_forecast_accuracy(actual_data, forecast):
    """
    Calcule les métriques de précision pour les prévisions.
    """
    try:
        # Fusionner les données réelles avec les prévisions
        comparison = pd.merge(
            actual_data[['ds', 'y']],
            forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']],
            on='ds',
            how='inner'
        )
        
        # Si aucune donnée commune, retourner des métriques par défaut
        if len(comparison) == 0:
            return {
                'MAPE': 0,
                'MAE': 0,
                'RMSE': 0,
                'Forecast Accuracy': 0,
                'Weighted Accuracy': 0
            }
        
        # Éviter division par zéro
        epsilon = 1e-10
        valid_comparison = comparison[comparison['y'] > epsilon].copy()
        
        if len(valid_comparison) == 0:
            return {
                'MAPE': 0,
                'MAE': 0,
                'RMSE': 0,
                'Forecast Accuracy': 0,
                'Weighted Accuracy': 0
            }
        
        # Mean Absolute Percentage Error (MAPE)
        valid_comparison['ape'] = abs((valid_comparison['y'] - valid_comparison['yhat']) / valid_comparison['y']) * 100
        mape = valid_comparison['ape'].mean()
        
        # Mean Absolute Error (MAE)
        mae = abs(comparison['y'] - comparison['yhat']).mean()
        
        # Root Mean Square Error (RMSE)
        rmse = np.sqrt(((comparison['y'] - comparison['yhat']) ** 2).mean())
        
        # Forecast Accuracy
        forecast_accuracy = 100 - mape
        
        # Weighted MAPE
        total_actual = valid_comparison['y'].sum()
        if total_actual > 0:
            valid_comparison['weighted_ape'] = valid_comparison['ape'] * (valid_comparison['y'] / total_actual)
            weighted_mape = valid_comparison['weighted_ape'].sum()
            weighted_accuracy = 100 - weighted_mape
        else:
            weighted_accuracy = 0
        
        # SMAPE
        valid_comparison['smape'] = 200 * abs(valid_comparison['y'] - valid_comparison['yhat']) / (abs(valid_comparison['y']) + abs(valid_comparison['yhat']) + epsilon)
        smape = valid_comparison['smape'].mean()
        
        return {
            'MAPE': mape,
            'MAE': mae,
            'RMSE': rmse,
            'SMAPE': smape,
            'Forecast Accuracy': forecast_accuracy,
            'Weighted Accuracy': weighted_accuracy
        }
    
    except Exception as e:
        print(f"Erreur lors du calcul des métriques: {str(e)}")
        traceback.print_exc()
        return {
            'MAPE': 0,
            'MAE': 0,
            'RMSE': 0,
            'Forecast Accuracy': 0,
            'Weighted Accuracy': 0
        }