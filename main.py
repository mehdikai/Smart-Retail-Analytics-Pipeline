"""
Script principal - Orchestration du pipeline Smart Retail Analytics
Exécute toutes les étapes : Intégration → Normalisation → Fédération → Visualisation
"""

import os
import sys
import time
from datetime import datetime
import pandas as pd
from typing import Dict, Tuple

# Import des modules du projet
from integration import load_all_sources, load_config, display_data_info
from normalisation import normalize_all_sources
from federation import federate_all, display_federation_info
from visualisation import create_all_visualizations


def print_banner():
    """Affiche la bannière du projet"""
    banner = """
    ╔════════════════════════════════════════════════════════════╗
    ║                                                            ║
    ║          SMART RETAIL ANALYTICS PIPELINE                   ║
    ║          Data Integration & Analysis System                ║
    ║                                                            ║
    ╚════════════════════════════════════════════════════════════╝
    """
    print(banner)
    print(f"    📅 Exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("    " + "="*60 + "\n")


def print_step(step_num: int, step_name: str):
    """Affiche le titre d'une étape"""
    print(f"\n{'='*60}")
    print(f"ÉTAPE {step_num} : {step_name.upper()}")
    print("="*60)


def save_processed_data(merged: pd.DataFrame, aggregations: Dict[str, pd.DataFrame], config: Dict):
    """
    Sauvegarde les données traitées
    
    Args:
        merged: DataFrame complet fusionné
        aggregations: Dictionnaire des agrégations
        config: Configuration
    """
    print("\n📁 Sauvegarde des données traitées...")
    
    # Créer le dossier si nécessaire
    processed_path = config['output_paths']['processed_data']
    os.makedirs(processed_path, exist_ok=True)
    
    # Sauvegarder le dataset complet
    merged_file = os.path.join(processed_path, 'data_complete.csv')
    merged.to_csv(merged_file, index=False)
    print(f"   ✓ Dataset complet: {merged_file}")
    
    # Sauvegarder les agrégations
    for name, df in aggregations.items():
        agg_file = os.path.join(processed_path, f'{name}.csv')
        df.to_csv(agg_file, index=False)
        print(f"   ✓ Agrégation {name}: {agg_file}")
    
    print(f"\n   ✅ {len(aggregations) + 1} fichiers sauvegardés dans: {processed_path}")


def generate_summary_report(merged: pd.DataFrame, aggregations: Dict[str, pd.DataFrame], 
                           execution_time: float, config: Dict):
    """
    Génère un rapport résumé
    
    Args:
        merged: DataFrame complet
        aggregations: Dictionnaire des agrégations
        execution_time: Temps d'exécution total
        config: Configuration
    """
    print("\n📝 Génération du rapport résumé...")
    
    # Créer le dossier si nécessaire
    report_path = config['output_paths']['report']
    os.makedirs(report_path, exist_ok=True)
    
    # Créer le rapport
    report_file = os.path.join(report_path, 'execution_summary.txt')
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("SMART RETAIL ANALYTICS - RAPPORT D'EXÉCUTION\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Temps d'exécution: {execution_time:.2f} secondes\n\n")
        
        f.write("-"*70 + "\n")
        f.write("STATISTIQUES DES DONNÉES\n")
        f.write("-"*70 + "\n\n")
        
        # Dataset complet
        f.write(f"Dataset fusionné:\n")
        f.write(f"  • Lignes: {len(merged):,}\n")
        f.write(f"  • Colonnes: {len(merged.columns)}\n")
        f.write(f"  • Période: {merged['date'].min()} à {merged['date'].max()}\n\n")
        
        # Agrégations
        f.write("Agrégations créées:\n")
        for name, df in aggregations.items():
            f.write(f"  • {name}: {len(df):,} lignes\n")
        
        f.write("\n" + "-"*70 + "\n")
        f.write("INSIGHTS CLÉS\n")
        f.write("-"*70 + "\n\n")
        
        # Top 5 campagnes
        f.write("Top 5 Campagnes Marketing:\n")
        top_campaigns = aggregations['campaign_sales'].head(5)
        for idx, row in top_campaigns.iterrows():
            f.write(f"  {idx+1}. {row['campaign_name']}: {row['total_amount']:,.2f}€ "
                   f"({row['order_count']} commandes)\n")
        
        f.write("\n")
        
        # Top 5 produits
        f.write("Top 5 Produits:\n")
        top_products = aggregations['product_sales'].head(5)
        for idx, row in top_products.iterrows():
            f.write(f"  {idx+1}. Produit #{row['product_id']}: {row['total_amount']:,.2f}€ "
                   f"({row['quantity']} unités)\n")
        
        f.write("\n")
        
        # Top 5 pays
        f.write("Top 5 Pays:\n")
        top_countries = aggregations['country_sales'].head(5)
        for idx, row in top_countries.iterrows():
            f.write(f"  {idx+1}. {row['country']}: {row['total_amount']:,.2f}€ "
                   f"({row['order_count']} commandes)\n")
        
        f.write("\n" + "-"*70 + "\n")
        f.write("FICHIERS GÉNÉRÉS\n")
        f.write("-"*70 + "\n\n")
        
        f.write("Données traitées:\n")
        f.write(f"  • {config['output_paths']['processed_data']}\n\n")
        
        f.write("Visualisations:\n")
        f.write(f"  • ventes_par_jour.png\n")
        f.write(f"  • ventes_par_campagne.png\n")
        f.write(f"  • trafic_vs_ventes.png\n")
        f.write(f"  Emplacement: {config['output_paths']['figures']}\n\n")
        
        f.write("="*70 + "\n")
        f.write("FIN DU RAPPORT\n")
        f.write("="*70 + "\n")
    
    print(f"   ✓ Rapport sauvegardé: {report_file}")


def main():
    """
    Fonction principale - Exécute le pipeline complet
    """
    start_time = time.time()
    
    try:
        # Bannière
        print_banner()
        
        # ÉTAPE 1 : CHARGEMENT DE LA CONFIGURATION
        print_step(1, "Chargement de la configuration")
        config = load_config()
        print("✓ Configuration chargée avec succès")
        
        # ÉTAPE 2 : INTÉGRATION DES SOURCES
        print_step(2, "Intégration des sources de données")
        raw_data = load_all_sources(config)
        print("\n✓ Toutes les sources chargées avec succès")
        
        # ÉTAPE 3 : NORMALISATION
        print_step(3, "Normalisation des données")
        normalized_data = normalize_all_sources(raw_data)
        print("\n✓ Normalisation terminée avec succès")
        
        # ÉTAPE 4 : FÉDÉRATION
        print_step(4, "Fédération des données")
        merged, aggregations = federate_all(normalized_data)
        print("\n✓ Fédération terminée avec succès")
        
        # ÉTAPE 5 : VISUALISATION
        print_step(5, "Création des visualisations")
        filepaths = create_all_visualizations(aggregations, config)
        print("\n✓ Visualisations créées avec succès")
        
        # ÉTAPE 6 : SAUVEGARDE
        print_step(6, "Sauvegarde des résultats")
        save_processed_data(merged, aggregations, config)
        
        # ÉTAPE 7 : RAPPORT
        execution_time = time.time() - start_time
        generate_summary_report(merged, aggregations, execution_time, config)
        
        # RÉSUMÉ FINAL
        print("\n" + "="*60)
        print("EXÉCUTION TERMINÉE AVEC SUCCÈS")
        print("="*60)
        print(f"\n⏱️  Temps total: {execution_time:.2f} secondes")
        print(f"📊 Dataset: {len(merged):,} lignes, {len(merged.columns)} colonnes")
        print(f"📈 Graphiques: {len(filepaths)} créés")
        print(f"📁 Agrégations: {len(aggregations)} sauvegardées")
        print(f"\n✅ Pipeline exécuté avec succès!")
        print(f"\n📂 Consultez les résultats dans:")
        print(f"   • Données: {config['output_paths']['processed_data']}")
        print(f"   • Figures: {config['output_paths']['figures']}")
        print(f"   • Rapport: {config['output_paths']['report']}")
        print("\n" + "="*60 + "\n")
        
        return 0  # Succès
        
    except Exception as e:
        print(f"\n❌ ERREUR LORS DE L'EXÉCUTION DU PIPELINE")
        print(f"❌ {str(e)}")
        import traceback
        traceback.print_exc()
        return 1  # Échec


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)