# pip install --upgrade selenium pandas apache-airflow apache-airflow-providers-postgres psycopg2-binary

import tempfile
import shutil
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# Définition des arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Configuration de la connexion 
BANK_CONN_ID = "banques_db_conn"

# Création du DAG
dag = DAG(
    'Banque_Collecting_Dag',
    default_args=default_args,
    schedule_interval='@weekly',
)

def scrape_reviews(**kwargs):
    """Scraper les avis Google Maps et stocker en CSV"""

    temp_user_data = tempfile.mkdtemp()

    options = webdriver.ChromeOptions()

    options.add_argument("--user-data-dir=/tmp/chrome_user_data")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)
    
    Villes = [
        "Casablanca",
        "Rabat",
        "Tanger",
        "Fès",
        "Marrakech",
        "Agadir",
        "Oujda",
        "Kenitra",
        "Mohammédia",
    ]

    Banks = [
        "Attijariwafa Bank",
        "AL BARID BANK",
        "BANQUE POPULAIRE",
        "BANK OF AFRICA",
        "BMCI BANK",
        "CREDIT AGRICOLE",
        "CIH BANK",
        "Crédit du Maroc",
        "Société générale Maroc",
        "BANK ASSAFA",
        "AL AKHDAR BANK",
        "BANK AL KARAM",
        "BANK AL YOUSR",
        "UMNIA BANK"
    ]
    
    all_data = []

    for ville in Villes:
        for bank in Banks:
            search_url = f"https://www.google.com/maps/search/{bank.replace(' ', '+')}+{ville}/"
            driver.get(search_url)
            time.sleep(2)

            try:
                scrollable = driver.find_element(By.CLASS_NAME, "m6QErb.DxyBCb.kA9KIf")
                for _ in range(10):
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                    time.sleep(1)
            except Exception:
                continue
            
            try:
                agency_elements = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                agency_links = [elem.get_attribute("href") for elem in agency_elements]
            except Exception:
                continue
            
            for agency_link in agency_links:
                driver.get(agency_link)
                time.sleep(2)

                try:
                    address_element = wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc")
                        )
                    )
                    address = address_element.text.strip()
                except TimeoutException:
                    address = "Non trouvée"

                try:
                    bank_name_element = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "span.a5H0ec"))
                    )
                    branch_name = bank_name_element.text.strip()+address
                except TimeoutException:
                    branch_name = bank+address
                
                try:
                    avis_button = wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Avis')]"))
                    )
                    avis_button.click()
                    time.sleep(2)
                except:
                    continue

                try:
                    container = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium"))
                    )
                except Exception as e:
                    continue

                scroll_attempts = 0
                while scroll_attempts < 30:
                    reviews_elements = driver.find_elements(By.CSS_SELECTOR, "div.MyEned")
                    if len(reviews_elements) >= 10 or scroll_attempts == 29:
                        break
                    driver.execute_script("window.scrollBy(0, 400);")
                    time.sleep(1)
                    scroll_attempts += 1

                reviews_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium")

                for review in reviews_elements[:10]:
                    try:
                        try:
                            more_button = review.find_element(By.CSS_SELECTOR, "button.w8nwRe.kyuRq")
                            driver.execute_script("arguments[0].click();", more_button)
                            time.sleep(0.5)
                        except Exception:
                            pass

                        review_text = review.find_element(By.CSS_SELECTOR, "span.wiI7pd").text.strip()

                        nbr_etoile_element = review.find_element(By.CSS_SELECTOR, "span.kvMYJc")
                        rating = (nbr_etoile_element.get_attribute("aria-label") 
                            if nbr_etoile_element else "Pas de note")
                        
                        date_text = review.find_element(By.CSS_SELECTOR, "span.rsqaWe")
                        date_review = (date_text.text.strip()
                            if date_text else "Inconnu")
                        
                        all_data.append((bank, branch_name, address, review_text, rating, date_review))
                    except:
                        continue

    driver.quit()
    shutil.rmtree(temp_user_data)
    
    df = pd.DataFrame(all_data, columns=["Bank_name", "Branch_name", "Location", "Review_text", "Rating", "Review_date"])
    csv_filename = "/tmp/avis_banques_maroc.csv"
    df.to_csv(csv_filename, index=False, encoding="utf-8")
    return csv_filename

def load_to_postgres(**kwargs):
    # Récupérer le chemin du fichier CSV via XCom
    ti = kwargs['ti']
    csv_file = ti.xcom_pull(task_ids='scrape_reviews')
    
    # Connexion via PostgresHook en utilisant le conn_id directement
    postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # Création de la table staging si elle n'existe pas
    create_table_query = """
    CREATE TABLE IF NOT EXISTS reviews (
        Bank_name TEXT,
        Branch_name TEXT,
        Location TEXT,
        Review_text TEXT,
        Rating TEXT,
        Review_date TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Chargement des données dans PostgreSQL avec copy_expert
    with open(csv_file, 'r', encoding='utf-8') as f:
        next(f)  # Ignorer l'en-tête
        cur.copy_expert("COPY staging_reviews FROM STDIN WITH CSV", f)
    conn.commit()
    cur.close()
    conn.close()

# Définition des tâches du DAG
scrape_task = PythonOperator(
    task_id='scrape_reviews',
    python_callable=scrape_reviews,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

scrape_task >> load_task