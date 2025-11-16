"""
Module de visualisation des données
Crée des graphiques pour l'analyse des ventes
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import yaml
import os
from typing import Dict, Tuple


def load_config(config_path: str = "config.yaml") -> Dict:
    """Charge le fichier de configuration"""
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def setup_plot_style(config: Dict) -> None:
    """Configure le style des graphiques"""
    plt.style.use('seaborn-v0_8-darkgrid')
    sns.set_palette("husl")


def plot_daily_sales(daily_sales: pd.DataFrame, output_path: str) -> str:
    """
    Graphique 1: Ventes par jour
    
    Args:
        daily_sales: DataFrame des ventes quotidiennes
        output_path: Dossier de sortie
    
    Returns:
        Chemin du fichier généré
    """
    print("\n📊 Création du graphique: Ventes par jour...")
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Tracer les ventes quotidiennes
    ax.plot(daily_sales['date'], daily_sales['total_amount'], 
            linewidth=2, color='#2E86AB', label='Chiffre d\'affaires')
    
    # Ajouter une ligne de tendance
    z = np.polyfit(range(len(daily_sales)), daily_sales['total_amount'], 1)
    p = np.poly1d(z)
    ax.plot(daily_sales['date'], p(range(len(daily_sales))), 
            "--", color='red', alpha=0.8, linewidth=2, label='Tendance')
    
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Chiffre d\'affaires (€)', fontsize=12, fontweight='bold')
    ax.set_title('Évolution des ventes quotidiennes', fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Rotation des dates
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    # Sauvegarder
    filepath = os.path.join(output_path, 'ventes_par_jour.png')
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"   ✓ Graphique sauvegardé: {filepath}")
    return filepath


def plot_campaign_sales(campaign_sales: pd.DataFrame, output_path: str, top_n: int = 15) -> str:
    """
    Graphique 2: Ventes par campagne (top campagnes)
    
    Args:
        campaign_sales: DataFrame des ventes par campagne
        output_path: Dossier de sortie
        top_n: Nombre de campagnes à afficher
    
    Returns:
        Chemin du fichier généré
    """
    print("\n📊 Création du graphique: Ventes par campagne...")
    
    # Prendre les top N campagnes
    top_campaigns = campaign_sales.head(top_n)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Créer le barplot horizontal
    bars = ax.barh(top_campaigns['campaign_name'], top_campaigns['total_amount'], 
                   color=sns.color_palette("coolwarm", len(top_campaigns)))
    
    # Ajouter les valeurs sur les barres
    for i, (bar, value) in enumerate(zip(bars, top_campaigns['total_amount'])):
        ax.text(value + 500, bar.get_y() + bar.get_height()/2, 
                f'{value:,.0f}€', 
                va='center', fontsize=9)
    
    ax.set_xlabel('Chiffre d\'affaires (€)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Campagne Marketing', fontsize=12, fontweight='bold')
    ax.set_title(f'Top {top_n} campagnes marketing par CA', fontsize=14, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    
    # Sauvegarder
    filepath = os.path.join(output_path, 'ventes_par_campagne.png')
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"   ✓ Graphique sauvegardé: {filepath}")
    return filepath


def plot_traffic_vs_sales(daily_sales: pd.DataFrame, output_path: str) -> str:
    """
    Graphique 3: Corrélation Trafic Web vs Ventes
    
    Args:
        daily_sales: DataFrame des ventes quotidiennes avec trafic
        output_path: Dossier de sortie
    
    Returns:
        Chemin du fichier généré
    """
    print("\n📊 Création du graphique: Trafic vs Ventes...")
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Graphique 1: Évolution temporelle (deux axes Y)
    color1 = '#2E86AB'
    color2 = '#A23B72'
    
    ax1_twin = ax1.twinx()
    
    # Tracer les ventes (axe gauche)
    line1 = ax1.plot(daily_sales['date'], daily_sales['total_amount'], 
                     linewidth=2, color=color1, label='Ventes (€)', alpha=0.8)
    ax1.set_ylabel('Chiffre d\'affaires (€)', fontsize=11, fontweight='bold', color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    
    # Tracer les sessions (axe droite)
    line2 = ax1_twin.plot(daily_sales['date'], daily_sales['sessions'], 
                          linewidth=2, color=color2, label='Sessions Web', alpha=0.8)
    ax1_twin.set_ylabel('Nombre de sessions', fontsize=11, fontweight='bold', color=color2)
    ax1_twin.tick_params(axis='y', labelcolor=color2)
    
    # Titre et légende
    ax1.set_xlabel('Date', fontsize=11, fontweight='bold')
    ax1.set_title('Évolution du trafic web et des ventes', fontsize=13, fontweight='bold', pad=15)
    
    # Combiner les légendes
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left', fontsize=10)
    
    ax1.grid(True, alpha=0.3)
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Graphique 2: Scatter plot (corrélation)
    ax2.scatter(daily_sales['sessions'], daily_sales['total_amount'], 
                alpha=0.6, s=50, color='#F18F01', edgecolors='black', linewidth=0.5)
    
    # Ligne de régression
    z = np.polyfit(daily_sales['sessions'], daily_sales['total_amount'], 1)
    p = np.poly1d(z)
    ax2.plot(daily_sales['sessions'], p(daily_sales['sessions']), 
             "--", color='red', linewidth=2, alpha=0.8, label='Régression linéaire')
    
    # Calculer la corrélation
    correlation = daily_sales['sessions'].corr(daily_sales['total_amount'])
    
    ax2.set_xlabel('Nombre de sessions web', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Chiffre d\'affaires (€)', fontsize=11, fontweight='bold')
    ax2.set_title(f'Corrélation Trafic Web ↔ Ventes (r = {correlation:.3f})', 
                  fontsize=13, fontweight='bold', pad=15)
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Sauvegarder
    filepath = os.path.join(output_path, 'trafic_vs_ventes.png')
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"   ✓ Graphique sauvegardé: {filepath}")
    return filepath


def create_all_visualizations(aggregations: Dict[str, pd.DataFrame], 
                              config: Dict = None) -> Dict[str, str]:
    """
    Crée toutes les visualisations
    
    Args:
        aggregations: Dictionnaire des agrégations
        config: Configuration (optionnel)
    
    Returns:
        Dictionnaire des chemins des fichiers générés
    """
    if config is None:
        config = load_config()
    
    print("\n" + "="*50)
    print("CRÉATION DES VISUALISATIONS")
    print("="*50)
    
    # Créer le dossier de sortie si nécessaire
    output_path = config['output_paths']['figures']
    os.makedirs(output_path, exist_ok=True)
    
    # Configurer le style
    setup_plot_style(config)
    
    # Importer numpy pour les régressions
    import numpy as np
    globals()['np'] = np
    
    # Créer les graphiques
    filepaths = {}
    
    filepaths['daily_sales'] = plot_daily_sales(
        aggregations['daily_sales'], 
        output_path
    )
    
    filepaths['campaign_sales'] = plot_campaign_sales(
        aggregations['campaign_sales'], 
        output_path
    )
    
    filepaths['traffic_vs_sales'] = plot_traffic_vs_sales(
        aggregations['daily_sales'], 
        output_path
    )
    
    print("\n" + "="*50)
    print("VISUALISATIONS TERMINÉES")
    print("="*50)
    print(f"\n✓ {len(filepaths)} graphiques créés dans: {output_path}")
    
    return filepaths


if __name__ == "__main__":
    # Test du module
    try:
        # Importer les modules précédents
        from integration import load_all_sources, load_config
        from normalisation import normalize_all_sources
        from federation import federate_all
        
        # Charger et traiter les données
        print("Chargement et traitement des données...")
        config = load_config()
        raw_data = load_all_sources(config)
        normalized_data = normalize_all_sources(raw_data)
        merged, aggregations = federate_all(normalized_data)
        
        # Créer les visualisations
        filepaths = create_all_visualizations(aggregations, config)
        
        print("\n✅ Test de visualisation réussi!")
        print("\n📁 Fichiers générés:")
        for name, path in filepaths.items():
            print(f"   • {name}: {path}")
        
    except Exception as e:
        print(f"\n❌ Erreur lors du test: {e}")
        import traceback
        traceback.print_exc()
        raise