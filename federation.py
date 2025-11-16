"""
Module de fédération des données
Joint les ventes, campagnes marketing, trafic web et données IoT
"""

import pandas as pd
import yaml
from typing import Dict, Tuple


def load_config(config_path: str = "config.yaml") -> Dict:
    """Charge le fichier de configuration"""
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def merge_orders_marketing(orders: pd.DataFrame, marketing: pd.DataFrame) -> pd.DataFrame:
    """
    Joint les ventes avec les campagnes marketing
    Associe chaque commande à une campagne active à sa date
    
    Args:
        orders: DataFrame des commandes normalisées
        marketing: DataFrame marketing normalisé
    
    Returns:
        DataFrame fusionné orders + marketing
    """
    print("📍 Jointure Orders ↔ Marketing...")
    
    # Créer une colonne date pour orders si elle n'existe pas
    if 'date' not in orders.columns:
        orders['date'] = orders['order_date']
    
    # Effectuer une jointure sur product_id
    merged = orders.merge(
        marketing,
        on='product_id',
        how='left'
    )
    
    # Filtrer pour garder seulement les commandes pendant les campagnes actives
    merged['in_campaign'] = (
        (merged['date'] >= merged['start_date']) & 
        (merged['date'] <= merged['end_date'])
    )
    
    # Marquer les commandes sans campagne
    merged['campaign_name'] = merged.apply(
        lambda row: row['campaign_name'] if row['in_campaign'] else 'No Campaign',
        axis=1
    )
    
    print(f"   ✓ {len(merged)} lignes après jointure")
    print(f"   ✓ {merged['in_campaign'].sum()} commandes liées à une campagne")
    print(f"   ✓ {(~merged['in_campaign']).sum()} commandes sans campagne")
    
    return merged


def merge_with_web_traffic(data: pd.DataFrame, web_traffic: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute les données de trafic web par date
    
    Args:
        data: DataFrame orders+marketing
        web_traffic: DataFrame trafic web normalisé
    
    Returns:
        DataFrame fusionné avec trafic web
    """
    print("\n📍 Jointure avec Web Traffic...")
    
    # Agréger le trafic web par date (au cas où plusieurs sources par jour)
    web_agg = web_traffic.groupby('date').agg({
        'pageviews': 'sum',
        'sessions': 'sum'
    }).reset_index()
    
    # Joindre sur la date
    merged = data.merge(
        web_agg,
        on='date',
        how='left'
    )
    
    # Remplir les valeurs manquantes par 0 (jours sans trafic)
    merged['pageviews'] = merged['pageviews'].fillna(0).astype(int)
    merged['sessions'] = merged['sessions'].fillna(0).astype(int)
    
    print(f"   ✓ {len(merged)} lignes après jointure")
    print(f"   ✓ Trafic web ajouté pour {merged['pageviews'].notna().sum()} jours")
    
    return merged


def merge_with_iot(data: pd.DataFrame, iot: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute les données IoT par date
    
    Args:
        data: DataFrame orders+marketing+web
        iot: DataFrame IoT normalisé
    
    Returns:
        DataFrame fusionné complet
    """
    print("\n📍 Jointure avec IoT...")
    
    # Créer colonne date pour IoT si elle n'existe pas
    if 'date' not in iot.columns:
        iot['date'] = iot['timestamp'].dt.date
        iot['date'] = pd.to_datetime(iot['date'])
    
    # Agréger IoT par date (moyenne de footfall et température)
    iot_agg = iot.groupby('date').agg({
        'footfall': 'mean',
        'temperature': 'mean'
    }).reset_index()
    
    # Joindre sur la date
    merged = data.merge(
        iot_agg,
        on='date',
        how='left'
    )
    
    # Remplir les valeurs manquantes par la moyenne
    merged['footfall'] = merged['footfall'].fillna(merged['footfall'].mean())
    merged['temperature'] = merged['temperature'].fillna(merged['temperature'].mean())
    
    print(f"   ✓ {len(merged)} lignes après jointure")
    print(f"   ✓ Données IoT ajoutées")
    
    return merged


def create_aggregations(data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Crée des agrégations utiles pour l'analyse
    
    Args:
        data: DataFrame fusionné complet
    
    Returns:
        Dictionnaire d'agrégations
    """
    print("\n📍 Création des agrégations...")
    
    aggregations = {}
    
    # 1. Ventes par jour
    aggregations['daily_sales'] = data.groupby('date').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_date': 'count',
        'pageviews': 'first',
        'sessions': 'first'
    }).reset_index()
    aggregations['daily_sales'].rename(columns={'order_date': 'order_count'}, inplace=True)
    
    print(f"   ✓ Ventes par jour: {len(aggregations['daily_sales'])} jours")
    
    # 2. Ventes par campagne
    aggregations['campaign_sales'] = data[data['in_campaign']].groupby('campaign_name').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_date': 'count'
    }).reset_index()
    aggregations['campaign_sales'].rename(columns={'order_date': 'order_count'}, inplace=True)
    aggregations['campaign_sales'] = aggregations['campaign_sales'].sort_values('total_amount', ascending=False)
    
    print(f"   ✓ Ventes par campagne: {len(aggregations['campaign_sales'])} campagnes")
    
    # 3. Ventes par produit
    aggregations['product_sales'] = data.groupby('product_id').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_date': 'count'
    }).reset_index()
    aggregations['product_sales'].rename(columns={'order_date': 'order_count'}, inplace=True)
    aggregations['product_sales'] = aggregations['product_sales'].sort_values('total_amount', ascending=False)
    
    print(f"   ✓ Ventes par produit: {len(aggregations['product_sales'])} produits")
    
    # 4. Ventes par pays
    aggregations['country_sales'] = data.groupby('country').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_date': 'count'
    }).reset_index()
    aggregations['country_sales'].rename(columns={'order_date': 'order_count'}, inplace=True)
    aggregations['country_sales'] = aggregations['country_sales'].sort_values('total_amount', ascending=False)
    
    print(f"   ✓ Ventes par pays: {len(aggregations['country_sales'])} pays")
    
    return aggregations


def federate_all(data: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Fédère toutes les sources de données
    
    Args:
        data: Dictionnaire des DataFrames normalisés
    
    Returns:
        Tuple (DataFrame complet, Dictionnaire d'agrégations)
    """
    print("\n" + "="*50)
    print("FÉDÉRATION DES DONNÉES")
    print("="*50 + "\n")
    
    # Étape 1: Orders + Marketing
    merged = merge_orders_marketing(data['orders'], data['marketing'])
    
    # Étape 2: + Web Traffic
    merged = merge_with_web_traffic(merged, data['web_traffic'])
    
    # Étape 3: + IoT
    merged = merge_with_iot(merged, data['iot'])
    
    # Étape 4: Créer agrégations
    aggregations = create_aggregations(merged)
    
    print("\n" + "="*50)
    print("FÉDÉRATION TERMINÉE")
    print("="*50)
    print(f"\n✓ Dataset complet: {len(merged)} lignes, {len(merged.columns)} colonnes")
    print(f"✓ {len(aggregations)} agrégations créées")
    
    return merged, aggregations


def display_federation_info(merged: pd.DataFrame, aggregations: Dict[str, pd.DataFrame]) -> None:
    """
    Affiche des informations sur les données fédérées
    
    Args:
        merged: DataFrame complet
        aggregations: Dictionnaire d'agrégations
    """
    print("\n" + "="*50)
    print("APERÇU DES DONNÉES FÉDÉRÉES")
    print("="*50 + "\n")
    
    print("📊 DATASET COMPLET")
    print(f"   Lignes: {len(merged)}, Colonnes: {len(merged.columns)}")
    print(f"   Colonnes: {list(merged.columns)}")
    print(f"   Aperçu:\n{merged.head(3)}\n")
    
    print("\n📊 AGRÉGATIONS")
    for name, df in aggregations.items():
        print(f"\n   {name.upper()}:")
        print(f"   {len(df)} lignes")
        print(f"{df.head(3)}\n")


if __name__ == "__main__":
    # Test du module
    try:
        # Importer les modules précédents
        from integration import load_all_sources, load_config
        from normalisation import normalize_all_sources
        
        # Charger et normaliser les données
        print("Chargement et normalisation des données...")
        config = load_config()
        raw_data = load_all_sources(config)
        normalized_data = normalize_all_sources(raw_data)
        
        # Fédérer les données
        merged, aggregations = federate_all(normalized_data)
        
        # Afficher les informations
        display_federation_info(merged, aggregations)
        
        print("\n✅ Test de fédération réussi!")
        
    except Exception as e:
        print(f"\n❌ Erreur lors du test: {e}")
        import traceback
        traceback.print_exc()
        raise