"""
Module de normalisation des données
Nettoie les dates, types numériques et gère les valeurs manquantes
"""

import pandas as pd
import numpy as np
from typing import Dict
import yaml


def load_config(config_path: str = "config.yaml") -> Dict:
    """Charge le fichier de configuration"""
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def clean_currency(series: pd.Series) -> pd.Series:
    """
    Nettoie les colonnes monétaires (enlève $, €, etc.)
    
    Args:
        series: Série pandas contenant des valeurs monétaires
    
    Returns:
        Série avec valeurs numériques
    """
    return series.str.replace(r'[\$€,]', '', regex=True).astype(float)


def normalize_dates(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    """
    Convertit les colonnes de dates au format datetime
    
    Args:
        df: DataFrame à normaliser
        date_columns: Liste des colonnes contenant des dates
    
    Returns:
        DataFrame avec dates normalisées
    """
    df_copy = df.copy()
    
    for col in date_columns:
        if col in df_copy.columns:
            try:
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
                print(f"   ✓ {col} converti en datetime")
            except Exception as e:
                print(f"   ✗ Erreur sur {col}: {e}")
    
    return df_copy


def normalize_numeric(df: pd.DataFrame, numeric_columns: list) -> pd.DataFrame:
    """
    Convertit les colonnes numériques au bon type
    
    Args:
        df: DataFrame à normaliser
        numeric_columns: Liste des colonnes numériques
    
    Returns:
        DataFrame avec types numériques corrects
    """
    df_copy = df.copy()
    
    for col in numeric_columns:
        if col in df_copy.columns:
            try:
                # Si c'est une colonne monétaire
                if df_copy[col].dtype == 'object' and '$' in str(df_copy[col].iloc[0]):
                    df_copy[col] = clean_currency(df_copy[col])
                else:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                print(f"   ✓ {col} converti en numérique")
            except Exception as e:
                print(f"   ✗ Erreur sur {col}: {e}")
    
    return df_copy


def handle_missing_values(df: pd.DataFrame, strategy: str = 'mean') -> pd.DataFrame:
    """
    Gère les valeurs manquantes selon une stratégie
    
    Args:
        df: DataFrame à traiter
        strategy: Stratégie ('mean', 'median', 'zero', 'drop')
    
    Returns:
        DataFrame sans valeurs manquantes
    """
    df_copy = df.copy()
    missing_before = df_copy.isnull().sum().sum()
    
    if missing_before == 0:
        print("   ✓ Aucune valeur manquante")
        return df_copy
    
    if strategy == 'drop':
        df_copy = df_copy.dropna()
        print(f"   ✓ {missing_before} lignes avec NA supprimées")
    elif strategy == 'zero':
        df_copy = df_copy.fillna(0)
        print(f"   ✓ {missing_before} valeurs manquantes remplacées par 0")
    elif strategy in ['mean', 'median']:
        numeric_cols = df_copy.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df_copy[col].isnull().any():
                fill_value = df_copy[col].mean() if strategy == 'mean' else df_copy[col].median()
                df_copy[col] = df_copy[col].fillna(fill_value)
        print(f"   ✓ Valeurs manquantes remplacées par {strategy}")
    
    return df_copy


def normalize_orders(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """Normalise la table orders"""
    print("\n1. Normalisation ORDERS...")
    df_clean = df.copy()
    
    # Dates
    df_clean = normalize_dates(df_clean, ['order_date'])
    
    # Numériques
    df_clean = normalize_numeric(df_clean, ['quantity', 'total_amount'])
    
    # Valeurs manquantes
    df_clean = handle_missing_values(df_clean, config['normalization']['fill_na_strategy'])
    
    # Ajouter une colonne date simple (sans heure) pour les jointures
    df_clean['date'] = df_clean['order_date'].dt.date
    
    print(f"   → {len(df_clean)} lignes après normalisation")
    return df_clean


def normalize_marketing(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """Normalise la table marketing"""
    print("\n2. Normalisation MARKETING...")
    df_clean = df.copy()
    
    # Dates
    df_clean = normalize_dates(df_clean, ['start_date', 'end_date'])
    
    # Valeurs manquantes
    df_clean = handle_missing_values(df_clean, config['normalization']['fill_na_strategy'])
    
    # Filtrer les campagnes invalides (end_date < start_date)
    invalid = df_clean[df_clean['end_date'] < df_clean['start_date']]
    if len(invalid) > 0:
        print(f"   ⚠ {len(invalid)} campagnes avec dates invalides supprimées")
        df_clean = df_clean[df_clean['end_date'] >= df_clean['start_date']]
    
    print(f"   → {len(df_clean)} lignes après normalisation")
    return df_clean


def normalize_web_traffic(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """Normalise la table web_traffic"""
    print("\n3. Normalisation WEB_TRAFFIC...")
    df_clean = df.copy()
    
    # Dates
    df_clean = normalize_dates(df_clean, ['date'])
    
    # Numériques
    df_clean = normalize_numeric(df_clean, ['pageviews', 'sessions'])
    
    # Valeurs manquantes
    df_clean = handle_missing_values(df_clean, config['normalization']['fill_na_strategy'])
    
    # Convertir date en date simple
    df_clean['date'] = df_clean['date'].dt.date
    
    print(f"   → {len(df_clean)} lignes après normalisation")
    return df_clean


def normalize_iot(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """Normalise la table iot"""
    print("\n4. Normalisation IOT...")
    df_clean = df.copy()
    
    # Dates
    df_clean = normalize_dates(df_clean, ['timestamp'])
    
    # Numériques
    df_clean = normalize_numeric(df_clean, ['footfall', 'temperature'])
    
    # Valeurs manquantes
    df_clean = handle_missing_values(df_clean, config['normalization']['fill_na_strategy'])
    
    # Ajouter colonne date pour agrégations
    df_clean['date'] = df_clean['timestamp'].dt.date
    
    print(f"   → {len(df_clean)} lignes après normalisation")
    return df_clean


def normalize_all_sources(data: Dict[str, pd.DataFrame], config: Dict = None) -> Dict[str, pd.DataFrame]:
    """
    Normalise toutes les sources de données
    
    Args:
        data: Dictionnaire de DataFrames bruts
        config: Configuration (optionnel)
    
    Returns:
        Dictionnaire de DataFrames normalisés
    """
    if config is None:
        config = load_config()
    
    print("\n" + "="*50)
    print("NORMALISATION DES DONNÉES")
    print("="*50)
    
    normalized_data = {
        'orders': normalize_orders(data['orders'], config),
        'marketing': normalize_marketing(data['marketing'], config),
        'web_traffic': normalize_web_traffic(data['web_traffic'], config),
        'iot': normalize_iot(data['iot'], config)
    }
    
    print("\n" + "="*50)
    print("NORMALISATION TERMINÉE")
    print("="*50 + "\n")
    
    return normalized_data


if __name__ == "__main__":
    # Test du module
    from integration import load_all_sources, display_data_info
    
    try:
        print("Chargement des données brutes...")
        config = load_config()
        raw_data = load_all_sources(config)
        
        print("\n" + "="*50)
        print("DONNÉES BRUTES")
        print("="*50)
        display_data_info(raw_data)
        
        # Normaliser
        normalized_data = normalize_all_sources(raw_data, config)
        
        print("\n" + "="*50)
        print("DONNÉES NORMALISÉES")
        print("="*50)
        display_data_info(normalized_data)
        
        print("✅ Test de normalisation réussi!")
        
    except Exception as e:
        print(f"\n❌ Erreur lors du test: {e}")
        raise