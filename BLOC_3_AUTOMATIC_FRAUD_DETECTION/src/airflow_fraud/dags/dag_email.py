from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_detailed_report():
    # 1. RÉCUPÉRATION DES SECRETS (Noms exacts de ton fichier original)
    try:
        conn_string = Variable.get("NEON_CONN_STRING")
        smtp_user = Variable.get("SMTP_USER")
        smtp_pass = Variable.get("SMTP_PASS")
        destinataire = Variable.get("DESTINATAIRE")
    except Exception as e:
        print(f"❌ Erreur Variables Airflow : {e}")
        raise

    # 2. CONNEXION ET RÉCUPÉRATION DES DONNÉES (Intervalle élargi pour le test)
    rows = []
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        
        # On cherche sur les 3 derniers jours pour être SUR de trouver quelque chose
        query = """
            SELECT trans_date_trans_time, merchant, amt, category, prediction, first, last
            FROM fraud_logs
            WHERE trans_date_trans_time >= CURRENT_DATE - INTERVAL '3 days'
            ORDER BY trans_date_trans_time DESC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Erreur DB : {e}. On continue pour tester l'email.")

    # 3. LOGIQUE DE CONTENU (FORÇAGE DU TEST SI VIDE)
    if not rows:
        print("⚠️ AUCUNE DONNÉE TROUVÉE. GÉNÉRATION D'UN MAIL DE TEST FORCÉ.")
        # On crée une ligne de donnée fictive pour valider l'envoi
        rows = [(datetime.now(), "MARCHAND_TEST", 99.99, "Test_Cat", 1, "David", "Test")]
        is_test = True
    else:
        print(f"✅ {len(rows)} transactions trouvées.")
        is_test = False

    # 4. CONSTRUCTION DU TABLEAU HTML
    table_rows = ""
    fraudes_detectees = 0
    for row in rows:
        date_t, merchant, amt, cat, pred, first, last = row
        bg_color = "#ffe6e6" if pred == 1 else "#ffffff"
        if pred == 1: fraudes_detectees += 1
        
        table_rows += f"""
        <tr style="background-color: {bg_color}; border-bottom: 1px solid #ddd;">
            <td style="padding: 8px;">{date_t}</td>
            <td style="padding: 8px;">{first} {last}</td>
            <td style="padding: 8px;">{merchant}</td>
            <td style="padding: 8px;">{cat}</td>
            <td style="padding: 8px;">{amt} €</td>
            <td style="padding: 8px;"><b>{'FRAUDE' if pred == 1 else 'OK'}</b></td>
        </tr>
        """

    html_content = f"""
    <html>
    <body>
        <h2 style="color: #2c3e50;">{'[TEST] ' if is_test else ''}Rapport des transactions</h2>
        <p><b>Bilan :</b> {len(rows)} transactions affichées.</p>
        <table style="width: 100%; border-collapse: collapse; font-family: Arial, sans-serif;">
            <thead>
                <tr style="background-color: #f2f2f2; text-align: left;">
                    <th style="padding: 8px;">Date</th><th style="padding: 8px;">Client</th>
                    <th style="padding: 8px;">Marchand</th><th style="padding: 8px;">Catégorie</th>
                    <th style="padding: 8px;">Montant</th><th style="padding: 8px;">Statut</th>
                </tr>
            </thead>
            <tbody>{table_rows}</tbody>
        </table>
    </body>
    </html>
    """

    # 5. ENVOI SMTP AVEC CAPTURE D'ERREUR
    msg = MIMEMultipart()
    msg['Subject'] = f"{'🚨 TEST' if is_test else '📊'} Rapport Fraude - {datetime.now().strftime('%d/%m/%Y')}"
    msg['From'] = smtp_user
    msg['To'] = destinataire
    msg.attach(MIMEText(html_content, 'html'))

    try:
        print(f"Tentative d'envoi à {destinataire}...")
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, destinataire, msg.as_string())
        print(f"📧 Rapport envoyé avec succès !")
    except Exception as e:
        print(f"❌ ÉCHEC CRITIQUE DE L'ENVOI : {e}")
        raise # Force la tâche en rouge dans Airflow pour voir l'erreur

# --- DÉFINITION DU DAG ---
with DAG(
    'dag_email_report_daily',
    default_args={'owner': 'david', 'retries': 1},
    schedule='0 8 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='envoyer_rapport_complet',
        python_callable=send_detailed_report
    )