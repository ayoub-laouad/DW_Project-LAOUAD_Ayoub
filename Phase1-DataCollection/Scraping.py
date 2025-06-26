# =============================================================================
# Importation des bibliothèques nécessaires
# =============================================================================
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
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
    "NADOR",
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
    "UMNIA BANK",
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
        time.sleep(3)

        # Scrolling dans la liste des agences pour s'assurer que tous les résultats se chargent
        try:
            # CORRECTION: Améliorer le scrolling pour charger TOUTES les agences
            prev_agency_count = 0
            scroll_attempts = 0
            max_scroll_attempts = 20
            consecutive_no_change = 0
            
            print("🔄 Début du chargement des agences...")
            
            # Trouver le conteneur scrollable des agences - approche similaire aux avis
            scrollable_container = None
            possible_selectors = [
                "div.m6QErb.DxyBCb.kA9KIf.dS8AEf",  # Conteneur principal avec dS8AEf
                "div.m6QErb.DxyBCb.kA9KIf",         # Conteneur de base
                "div[role='main']",                   # Conteneur principal
                "div.Nv2PK.tH5CWc.THOPZb"           # Autre conteneur possible
            ]
            
            for selector in possible_selectors:
                try:
                    scrollable_container = driver.find_element(By.CSS_SELECTOR, selector)
                    print(f"✅ Conteneur scrollable des agences trouvé avec : {selector}")
                    break
                except:
                    continue
            
            while scroll_attempts < max_scroll_attempts:
                # Compter les agences actuelles
                current_agencies = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                current_count = len(current_agencies)
                
                if scroll_attempts % 5 == 0:  # Afficher le progrès tous les 5 scrolls
                    print(f"🔄 Scroll {scroll_attempts + 1}: {current_count} agences trouvées")
                
                # Si on n'a pas chargé de nouvelles agences
                if current_count == prev_agency_count:
                    consecutive_no_change += 1
                    # Arrêter seulement après plusieurs tentatives consécutives sans changement
                    if consecutive_no_change >= 5:
                        print(f"✅ Arrêt du scroll après {consecutive_no_change} tentatives sans nouvelles agences")
                        break
                else:
                    consecutive_no_change = 0  # Reset du compteur
                    prev_agency_count = current_count
                
                # Approche multiple pour le scroll - comme pour les avis
                if scrollable_container:
                    # Méthode 1: Scroller dans le conteneur des agences
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_container)
                else:
                    # Méthode 2: Fallback avec scroll général
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Méthode 3: Essayer aussi de scroller avec les touches (comme scroll manuel)
                try:
                    if current_agencies:
                        # Simuler un scroll en cliquant sur le dernier élément visible puis utiliser les touches
                        last_agency = current_agencies[-1]
                        driver.execute_script("arguments[0].scrollIntoView();", last_agency)
                        time.sleep(1)
                        # Simuler plusieurs appuis sur la touche PAGE_DOWN
                        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
                        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
                except:
                    pass
                
                time.sleep(2)  # Délai pour le chargement
                scroll_attempts += 1
                
        except Exception:
            print(f"❌ Aucune agence trouvée pour {bank} à {ville}, passage à la suivante.")
            continue

        # Récupérer tous les liens des agences APRÈS le scrolling complet
        agency_elements = driver.find_elements(By.CLASS_NAME, "hfpxzc")
        agency_links = [elem.get_attribute("href") for elem in agency_elements]
        print(f"✅ {len(agency_links)} agences trouvées pour {bank} à {ville}")

        # Pour chaque agence trouvée, nous extrayons les informations et les avis
        for agency_link in agency_links:
            print(f"\n➡️ Accès à l'agence : {agency_link}")
            if agency_link and isinstance(agency_link, str) and agency_link.startswith("http"):
                driver.get(agency_link)
                time.sleep(3)

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
                    branch_name = bank_name_element.text.strip() + " - " + address
                except TimeoutException:
                    branch_name = bank + " - " + address

                # Cliquer sur le bouton "Avis" pour accéder aux commentaires
                try:
                    avis_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Avis')]")))
                    avis_button.click()
                    time.sleep(3)
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

                # CORRECTION: Améliorer le scrolling pour charger TOUS les avis
                prev_count = 0
                scroll_attempts = 0
                max_scroll_attempts = 50
                consecutive_no_change = 0
                
                print("🔄 Début du chargement des avis...")
                
                # Trouver le conteneur scrollable des avis
                try:
                    # Plusieurs sélecteurs possibles pour le conteneur des avis
                    scrollable_container = None
                    possible_selectors = [
                        "div.m6QErb.DxyBCb.kA9KIf.dS8AEf",  # Conteneur principal des avis
                        "div[role='main']",  # Conteneur principal
                        "div.m6QErb.DxyBCb.kA9KIf"  # Variante du conteneur
                    ]
                    
                    for selector in possible_selectors:
                        try:
                            scrollable_container = driver.find_element(By.CSS_SELECTOR, selector)
                            print(f"✅ Conteneur scrollable trouvé avec : {selector}")
                            break
                        except:
                            continue
                            
                except Exception as e:
                    print(f"⚠️ Impossible de trouver le conteneur scrollable : {e}")
                    scrollable_container = None
                
                while scroll_attempts < max_scroll_attempts:
                    # Récupère tous les avis actuellement dans le DOM
                    reviews_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium")
                    current_count = len(reviews_elements)
                    
                    if scroll_attempts % 5 == 0:  # Afficher le progrès tous les 5 scrolls
                        print(f"🔄 Scroll {scroll_attempts + 1}: {current_count} avis chargés")
                    
                    # Si on n'a pas chargé de nouvel avis
                    if current_count == prev_count:
                        consecutive_no_change += 1
                        # Arrêter seulement après plusieurs tentatives consécutives sans changement
                        if consecutive_no_change >= 5:
                            print(f"✅ Arrêt du scroll après {consecutive_no_change} tentatives sans nouveaux avis")
                            break
                    else:
                        consecutive_no_change = 0  # Reset du compteur
                        prev_count = current_count
                    
                    # Scroller dans le bon conteneur
                    if scrollable_container:
                        # Scroller dans le conteneur des avis
                        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_container)
                    else:
                        # Fallback: scroller dans la page principale
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    
                    time.sleep(2)  # Délai pour le chargement
                    scroll_attempts += 1
            else:
                print(f"❌ Lien invalide ignoré : {agency_link}")

            # Mettre à jour la liste des avis chargés après le scrolling complet
            reviews_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium")
            print(f"🔍 Nombre total d'avis chargés pour l'agence : {len(reviews_elements)}")

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

                    # Extraire le texte de l'avis - Améliorer les sélecteurs
                    review_text = ""
                    possible_text_selectors = [
                        "span.wiI7pd",
                        "div.MyEned span",
                        "div.jftiEf span",
                        "span[data-expandable-section]"
                    ]
                    
                    for selector in possible_text_selectors:
                        try:
                            review_text = review.find_element(By.CSS_SELECTOR, selector).text.strip()
                            if review_text:  # Si on a trouvé du texte, on arrête
                                break
                        except:
                            continue
                    
                    if not review_text:  # Si aucun texte trouvé
                        print("⚠️ Texte d'avis non trouvé, passage au suivant")
                        continue
                    # Extraire la note - Améliorer les sélecteurs
                    nbr_etoile = "Pas de note"
                    possible_rating_selectors = [
                        "span.kvMYJc",
                        "div[role='img'][aria-label*='étoile']",
                        "span[aria-label*='étoile']"
                    ]
                    
                    for selector in possible_rating_selectors:
                        try:
                            nbr_etoile_element = review.find_element(By.CSS_SELECTOR, selector)
                            nbr_etoile = nbr_etoile_element.get_attribute("aria-label")
                            if nbr_etoile:
                                break
                        except:
                            continue
                    # Extraire la date de l'avis - Améliorer les sélecteurs
                    review_date = "Inconnu"
                    possible_date_selectors = [
                        "span.rsqaWe",
                        "span.DU9Pgb",
                        "div.rsqaWe"
                    ]
                    
                    for selector in possible_date_selectors:
                        try:
                            date_elements = review.find_elements(By.CSS_SELECTOR, selector)
                            if date_elements:
                                review_date = date_elements[0].text.strip()
                                break
                        except:
                            continue

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
