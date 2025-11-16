"""
Module d'orchestration - Exécution automatique du pipeline
Exécute main.py à 15:00 GMT et envoie un rapport par email
for testing : python orchestration.py --test
for service runing : python orchestration.py
"""

import schedule
import time
import smtplib
import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import logging
import pytz

# Import du pipeline principal
from main import main as run_pipeline


# ============================================================
# CONFIGURATION
# ============================================================

# Email de destination
RECIPIENT_EMAIL = "elkaissounielmehdi@gmail.com"

# Heure d'exécution (GMT)
EXECUTION_TIME = "15:00"

# Configuration Email (à personnaliser)
SMTP_SERVER = "smtp.gmail.com"  # Pour Gmail
SMTP_PORT = 587
SENDER_EMAIL = "kaissounim61@gmail.com"  
SENDER_PASSWORD = "rpex cvax dfbs ofyl"  

# Fichiers
LOG_FILE = "logs/orchestration.log"
REPORT_FILE = "outputs/report/execution_summary.txt"


# ============================================================
# CONFIGURATION DU LOGGING
# ============================================================

def setup_logging():
    """Configure le système de logging"""
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


# ============================================================
# FONCTION D'ENVOI D'EMAIL
# ============================================================

def send_email_with_report(report_path: str, execution_status: str, execution_time: float):
    """
    Envoie un email avec le rapport en pièce jointe
    
    Args:
        report_path: Chemin vers le fichier rapport
        execution_status: Statut de l'exécution (Success/Failed)
        execution_time: Temps d'exécution en secondes
    """
    try:
        logging.info("📧 Préparation de l'email...")
        
        # Créer le message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECIPIENT_EMAIL
        msg['Subject'] = f"Smart Retail Analytics - Rapport Quotidien [{execution_status}]"
        
        # Corps du message
        body = f"""
Bonjour,

Le pipeline Smart Retail Analytics a été exécuté avec succès.

📊 RÉSUMÉ DE L'EXÉCUTION
{'='*50}
• Date: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S GMT')}
• Statut: {execution_status}
• Temps d'exécution: {execution_time:.2f} secondes

Le rapport détaillé est joint à cet email.

📁 FICHIERS GÉNÉRÉS:
• Données traitées: data/processed/
• Visualisations: outputs/figures/
• Rapport: outputs/report/

---
Cet email a été généré automatiquement par le système d'orchestration.
Smart Retail Analytics Pipeline v1.0
        """
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Attacher le rapport si disponible
        if os.path.exists(report_path):
            with open(report_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(report_path)}'
                )
                msg.attach(part)
            logging.info(f"   ✓ Rapport attaché: {report_path}")
        else:
            logging.warning(f"   ⚠ Rapport non trouvé: {report_path}")
        
        # Envoyer l'email
        logging.info(f"   📤 Envoi à: {RECIPIENT_EMAIL}")
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        logging.info("   ✅ Email envoyé avec succès!")
        return True
        
    except Exception as e:
        logging.error(f"   ❌ Erreur lors de l'envoi de l'email: {e}")
        return False


# ============================================================
# FONCTION D'EXÉCUTION DU PIPELINE
# ============================================================

def execute_pipeline():
    """
    Exécute le pipeline et envoie le rapport par email
    """
    start_time = time.time()
    gmt_time = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S GMT')
    
    logging.info("="*60)
    logging.info(f"🚀 DÉMARRAGE DU PIPELINE - {gmt_time}")
    logging.info("="*60)
    
    try:
        # Exécuter le pipeline principal
        logging.info("▶️  Exécution de main.py...")
        exit_code = run_pipeline()
        
        execution_time = time.time() - start_time
        
        if exit_code == 0:
            logging.info(f"✅ Pipeline exécuté avec succès en {execution_time:.2f}s")
            status = "Success"
        else:
            logging.error(f"❌ Pipeline échoué avec code: {exit_code}")
            status = "Failed"
        
        # Envoyer l'email avec le rapport
        send_email_with_report(REPORT_FILE, status, execution_time)
        
        logging.info("="*60)
        logging.info(f"✅ EXÉCUTION TERMINÉE - {datetime.now(pytz.UTC).strftime('%H:%M:%S GMT')}")
        logging.info("="*60 + "\n")
        
    except Exception as e:
        execution_time = time.time() - start_time
        logging.error(f"❌ ERREUR CRITIQUE: {e}")
        
        # Essayer d'envoyer un email d'erreur
        try:
            send_email_with_report(REPORT_FILE, "Failed - Error", execution_time)
        except:
            logging.error("❌ Impossible d'envoyer l'email d'erreur")
        
        logging.info("="*60 + "\n")


# ============================================================
# MODE DAEMON (EXÉCUTION CONTINUE)
# ============================================================

def start_daemon():
    """
    Lance le daemon qui vérifie l'heure et exécute le pipeline à 19:45 GMT
    """
    logging.info("🔄 DÉMARRAGE DU DAEMON D'ORCHESTRATION")
    logging.info(f"⏰ Heure d'exécution configurée: {EXECUTION_TIME} GMT")
    logging.info(f"📧 Email de notification: {RECIPIENT_EMAIL}")
    logging.info(f"📝 Logs: {LOG_FILE}")
    logging.info("="*60 + "\n")
    
    # Planifier l'exécution quotidienne à 19:45 GMT
    schedule.every().day.at(EXECUTION_TIME).do(execute_pipeline)
    
    # Message de confirmation
    next_run = schedule.next_run()
    logging.info(f"⏳ Prochaine exécution: {next_run}")
    logging.info("💤 En attente...\n")
    
    # Boucle principale
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Vérifier toutes les minutes
            
    except KeyboardInterrupt:
        logging.info("\n⚠️  Arrêt du daemon demandé (Ctrl+C)")
        logging.info("👋 Daemon arrêté\n")


# ============================================================
# MODE TEST (EXÉCUTION IMMÉDIATE)
# ============================================================

def test_mode():
    """
    Mode test - Exécute immédiatement le pipeline et envoie l'email
    """
    logging.info("🧪 MODE TEST ACTIVÉ")
    logging.info("▶️  Exécution immédiate du pipeline...\n")
    execute_pipeline()


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Point d'entrée principal
    """
    import sys
    
    # Setup logging
    setup_logging()
    
    # Vérifier les arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Mode test
        test_mode()
    else:
        # Mode daemon (par défaut)
        print("\n" + "="*60)
        print("SMART RETAIL ANALYTICS - ORCHESTRATION DAEMON")
        print("="*60)
        print(f"⏰ Exécution quotidienne: {EXECUTION_TIME} GMT")
        print(f"📧 Notification: {RECIPIENT_EMAIL}")
        print(f"📝 Logs: {LOG_FILE}")
        print("\n💡 Pour tester immédiatement: python orchestration.py --test")
        print("🛑 Pour arrêter: Ctrl+C")
        print("="*60 + "\n")
        
        start_daemon()


if __name__ == "__main__":
    main()