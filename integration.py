"""
Module d'intégration des sources de données
Charge les données depuis SQLite, CSV et JSON
"""

import pandas as pd
import sqlite3
import yaml
import os
from typing import Dict, Optional


def load_config(config_path: str = "config.yaml") -> Dict:
    """Charge le fichier de configuration YAML"""
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config


def load_sqlite(db_path: str, table_name: str) -> pd.DataFrame:
    """
    Charge les données depuis une base SQLite
    
    Args:
        db_path: Chemin vers le fichier .db
        table_name: Nom de la table à charger
    
    Returns:
        DataFrame pandas avec les données
    """
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        print(f"✓ SQLite chargé: {len(df)} lignes depuis '{table_name}'")
        return df
    except Exception as e:
        print(f"✗ Erreur lors du chargement SQLite: {e}")
        raise


def load_csv(csv_path: str) -> pd.DataFrame:
    """
    Charge les données depuis un fichier CSV
    
    Args:
        csv_path: Chemin vers le fichier CSV
    
    Returns:
        DataFrame pandas avec les données
    """
    try:
        df = pd.read_csv(csv_path)
        print(f"✓ CSV chargé: {len(df)} lignes depuis '{os.path.basename(csv_path)}'")
        return df
    except Exception as e:
        print(f"✗ Erreur lors du chargement CSV: {e}")
        raise


def load_json(json_path: str) -> pd.DataFrame:
    """
    Charge les données depuis un fichier JSON
    
    Args:
        json_path: Chemin vers le fichier JSON
    
    Returns:
        DataFrame pandas avec les données
    """
    try:
        df = pd.read_json(json_path)
        print(f"✓ JSON chargé: {len(df)} lignes depuis '{os.path.basename(json_path)}'")
        return df
    except Exception as e:
        print(f"✗ Erreur lors du chargement JSON: {e}")
        raise


def load_all_sources(config: Optional[Dict] = None) -> Dict[str, pd.DataFrame]:
    """
    Charge toutes les sources de données du projet
    
    Args:
        config: Dictionnaire de configuration (optionnel)
    
    Returns:
        Dictionnaire contenant tous les DataFrames
    """
    if config is None:
        config = load_config()
    
    print("\n" + "="*50)
    print("CHARGEMENT DES SOURCES DE DONNÉES")
    print("="*50 + "\n")
    
    data = {}
    
    # Charger SQLite (orders)
    print("1. Chargement de la base SQLite...")
    data['orders'] = load_sqlite(
        config['data_paths']['sqlite_db'],
        config['sqlite']['table_name']
    )
    
    # Charger Marketing CSV
    print("\n2. Chargement du fichier marketing...")
    data['marketing'] = load_csv(config['data_paths']['marketing_csv'])
    
    # Charger Web Traffic JSON
    print("\n3. Chargement du trafic web...")
    data['web_traffic'] = load_json(config['data_paths']['web_traffic_json'])
    
    # Charger IoT Stream CSV
    print("\n4. Chargement des données IoT...")
    data['iot'] = load_csv(config['data_paths']['iot_stream_csv'])
    
    print("\n" + "="*50)
    print("CHARGEMENT TERMINÉ")
    print("="*50 + "\n")
    
    return data


def display_data_info(data: Dict[str, pd.DataFrame]) -> None:
    """
    Affiche des informations sur les données chargées
    
    Args:
        data: Dictionnaire de DataFrames
    """
    print("\n" + "="*50)
    print("APERÇU DES DONNÉES")
    print("="*50 + "\n")
    
    for name, df in data.items():
        print(f"📊 {name.upper()}")
        print(f"   Lignes: {len(df)}, Colonnes: {len(df.columns)}")
        print(f"   Colonnes: {list(df.columns)}")
        print(f"   Aperçu:\n{df.head(2)}\n")


if __name__ == "__main__":
    # Test du module
    try:
        # Charger la configuration
        config = load_config()
        
        # Charger toutes les sources
        data = load_all_sources(config)
        
        # Afficher les informations
        display_data_info(data)
        
        print("✅ Test d'intégration réussi!")
        
    except Exception as e:
        print(f"\n❌ Erreur lors du test: {e}")
        raise