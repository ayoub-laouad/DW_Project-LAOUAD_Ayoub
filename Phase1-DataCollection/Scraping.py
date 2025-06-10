# =============================================================================
# Importation des bibliothèques nécessaires
# =============================================================================
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import pandas as pd

# =============================================================================
# Configuration du WebDriver et des délais d'attente
# =============================================================================
# Nous configurons le ChromeDriver en mode non-headless afin de visualiser le déroulement.
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 15)

# =============================================================================
# Définition des villes et banques à rechercher
# =============================================================================
Villes = [
    "AGADIR",
    "AL HOCEIMA",
    "ASILAH",
    "BERKANE",
    "BENI MELLAL",
    "CASABLANCA",
    "EL JADIDA",
    "ESSAOUIRA",
    "ERRACHIDIA",
    "FÈS",
    "GUELMIM",
    "IFRANE",
    "KENITRA",
    "MARRAKECH",
    "MÉKNÈS",
    "M'DIQ",
    "MOHAMMEDIA",
    " ",
    "OUARZAZATE",
    "OUJDA",
    "RABAT",
    "SAFI",
    "SETTAT",
    "SIDI KACEM",
    "TAROUDANT",
    "TAZA",
    "TANGER",
    "TÉMARA",
    "TÉTOUAN",
    "TIZNIT"
]

Banks = [
    "ATTIJARIWAFA BANK",
    "AL BARID BANK",
    "BANQUE POPULAIRE",
    "BANQUE CENTRALE POPULAIRE",
    "BANK OF AFRICA",
    "BMCI",
    "BMCI BANK",         # Banque marocaine pour le commerce et l'industrie
    "CREDIT AGRICOLE",
    "CIH BANK",
    "CREDIT IMMOBILIER ET HOTELIER BANK",
    "Crédit du Maroc",
    "CREDIT DU MAROC",
    "SOCIÉTÉ GÉNÉRALE MAROC",
    "BANK ASSAFA",
    "AL AKHDAR BANK",
    "BANK AL KARAM",
    "BANK AL YOUSR",
    "UMNIA BANK"
    "ARAB BANK",
    "BTI BANK",
    "CDG CAPITAL",
    "CFG BANK",
    "CITIBANK MAGHREB",
    "DAR EL AMANE"
]

# Liste qui contiendra l'ensemble des données collectées
all_data = []
data_temp = 1

# =============================================================================
# Boucle principale pour le scraping
# =============================================================================
for ville in Villes:
    for bank in Banks:
        print(f"\n🔍 Recherche des agences pour : {bank} à {ville}")
        # Construire l'URL de recherche sur Google Maps
        search_url = f"https://www.google.com/maps/search/{bank.replace(' ', '+')}+{ville}/"
        driver.get(search_url)
        time.sleep(2)

        # Scrolling dans la liste des agences pour s'assurer que tous les résultats se chargent
        try:
            scrollable = driver.find_element(By.CLASS_NAME, "m6QErb.DxyBCb.kA9KIf")
            for _ in range(10):
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                time.sleep(1)
        except Exception:
            print(f"❌ Aucune agence trouvée pour {bank} à {ville}, passage à la suivante.")
            continue

        # Récupérer tous les liens des agences
        agency_elements = driver.find_elements(By.CLASS_NAME, "hfpxzc")
        agency_links = [elem.get_attribute("href") for elem in agency_elements]
        print(f"✅ {len(agency_links)} agences trouvées pour {bank} à {ville}")

        # Pour chaque agence trouvée, nous extrayons les informations et les avis
        for agency_link in agency_links:
            print(f"\n➡️ Accès à l'agence : {agency_link}")
            driver.get(agency_link)
            time.sleep(2)

            # Récupérer l'adresse de l'agence (si disponible)
            try:
                address_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc")
                    )
                )
                address = address_element.text.strip()
            except TimeoutException:
                address = "Non trouvée"

            # Récupérer le nom (branch) de l'agence
            try:
                bank_name_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.a5H0ec"))
                )
                branch_name = bank_name_element.text.strip()+address
            except TimeoutException:
                branch_name = bank+address

            # Cliquer sur le bouton "Avis" pour accéder aux commentaires
            try:
                avis_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Avis')]")))
                avis_button.click()
                time.sleep(2)
            except Exception as e:
                print(f"❌ Impossible de cliquer sur 'Avis' pour cette agence : {e}")
                continue

            # Attendre le chargement du conteneur d'avis
            try:
                container = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium"))
                )
            except Exception as e:
                print(f"❌ Conteneur d'avis non trouvé pour cette agence : {e}")
                continue

            # Scroller pour charger tout les avis
            prev_count = 0
            scroll_attempts = 0
            while True:
                # Récupère tous les avis actuellement dans le DOM
                reviews_elements = driver.find_elements(By.CSS_SELECTOR, "div.MyEned")
                current_count = len(reviews_elements)
                # Si on n'a pas chargé de nouvel avis depuis le dernier passage, on arrête
                if current_count == prev_count:
                    break
                # Sinon, on mémorise ce nouveau nombre et on scroll encore un peu
                prev_count = current_count
                driver.execute_script("window.scrollBy(0, 400);")
                time.sleep(1)  # petit délai pour laisser le temps aux nouveaux avis de charger
                scroll_attempts += 1
                if scroll_attempts >= 30:  # sécurité : ne pas scroller indéfiniment
                    break

            # Mettre à jour la liste des avis chargés   
            reviews_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium")
            print(f"🔍 Nombre d'avis chargés pour l'agence : {len(reviews_elements)}")

            # Extraction des avis de l'agence
            for review in reviews_elements:
                try:
                    # Si le texte est tronqué, cliquer sur le bouton "plus"
                    try:
                        more_button = review.find_element(By.CSS_SELECTOR, "button.w8nwRe.kyuRq")
                        driver.execute_script("arguments[0].click();", more_button)
                        time.sleep(0.5)
                    except Exception:
                        pass

                    # Extraire le texte de l'avis
                    review_text = review.find_element(By.CSS_SELECTOR, "span.wiI7pd").text.strip()
                    # Extraire la note à partir de l'attribut "aria-label" de l'élément "kvMYJc"
                    nbr_etoile_element = review.find_element(By.CSS_SELECTOR, "span.kvMYJc")
                    nbr_etoile = nbr_etoile_element.get_attribute("aria-label") if nbr_etoile_element else "Pas de note"
                    # Extraire la date de l'avis (si disponible)
                    review_date = (
                        review.find_element(By.CSS_SELECTOR, "span.rsqaWe").text.strip()
                        if review.find_elements(By.CSS_SELECTOR, "span.rsqaWe")
                        else "Inconnu"
                    )

                    # Ajouter les informations de cet avis à la liste globale
                    all_data.append({
                        "bank_name": bank,
                        "branch_name": branch_name,
                        "location": address,
                        "city": ville,
                        "review_text": review_text,
                        "rating": nbr_etoile,
                        "review_date": review_date
                    })

                    # --- Sauvegarde intermédiaire tous les 100 avis ---
                    if len(all_data) > (100*data_temp):
                        try:
                            df_temp = pd.DataFrame(all_data)
                            df_temp.to_csv("backup_temp.csv", index=False, encoding="utf-8")
                            print(f"💾 Sauvegarde intermédiaire : {len(all_data)} avis sauvegardés dans 'backup_temp.csv'")
                            data_temp += 1
                        except Exception as e_save:
                            print(f"⚠️ Impossible de faire la sauvegarde intermédiaire : {e_save}")
                            data_temp += 1
                except Exception as e:
                    print(f"⚠️ Erreur lors du traitement d'un avis pour cette agence : {e}")

            print(f"✅ Avis extraits pour cette agence : {len(reviews_elements)} avis.")

# =============================================================================
# Fermeture du WebDriver et sauvegarde des données dans un CSV
# =============================================================================
driver.quit()

df = pd.DataFrame(all_data)

try:
    csv_filename = "avis_banques_Maroc.csv"
    df.to_csv(csv_filename, index=False, encoding="utf-8")
    print(f"✅ Données sauvegardées dans '{csv_filename}'")
except Exception as e1:
    print(f"⚠️ Erreur lors de la première sauvegarde : {e1}")
    try:
        backup_path = "C:/Users/ayoub/Downloads/voice_conversion_app/avis_banques_Maroc_backup.csv"
        df.to_csv(backup_path, index=False, encoding="utf-8")
        print(f"✅ Données sauvegardées dans le fichier de secours : '{backup_path}'")
    except Exception as e2:
        print(f"❌ Échec de la sauvegarde CSV de secours : {e2}")
        json_path = "avis_banques_Maroc.json"
        df.to_json(json_path, orient="records", force_ascii=False, indent=2)
        print(f"✅ Données sauvegardées en JSON dans '{json_path}'")

print(f"\n✅ Données enregistrées dans '{csv_filename}' ({len(all_data)} avis).")
print("🚀 Scraping terminé pour toutes les banques !")
