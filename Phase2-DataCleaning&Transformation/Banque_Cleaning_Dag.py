# pip install apache-airflow apache-airflow-providers-postgres dbt-core dbt-postgres 
# pip install langdetect transformers torch scikit-learn gensim nltk pandas psycopg2-binary sqlalchemy

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import nltk
from langdetect import detect, DetectorFactory, LangDetectException
from transformers import pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import logging
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import errors as pg_errors
import re
import string
from sqlalchemy import create_engine
import warnings
import numpy as np
warnings.filterwarnings('ignore')

# ---------------------------------------------------------------------
# Configuration
BANK_CONN_ID = "banques_db_conn"
SQL_ALCHEMY_CONN = "postgresql+psycopg2://ayoub:ayoub@localhost:5432/bank_reviews"

# Configuration NLTK
def download_nltk_resources():
    """Télécharge les ressources NLTK nécessaires"""
    resources = ['punkt', 'punkt_tab', 'stopwords']
    for resource in resources:
        try:
            if resource == 'stopwords':
                nltk.data.find('corpora/stopwords')
            else:
                nltk.data.find(f'tokenizers/{resource}')
            logging.info(f"Ressource {resource} déjà disponible")
        except LookupError:
            try:
                nltk.download(resource, quiet=True)
                logging.info(f"Ressource {resource} téléchargée avec succès")
            except Exception as e:
                logging.warning(f"Impossible de télécharger {resource}: {e}")

download_nltk_resources()
DetectorFactory.seed = 0

# ---------------------------------------------------------------------
# Configuration DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 3, 18)
}

dag = DAG(
    'Banque_Cleaning_Transformation_DAG',
    schedule_interval='@weekly',
    default_args=default_args,
    catchup=False,
    description="DAG complet pour l'analyse enrichie des avis bancaires avec nettoyage de stopwords"
)

# ---------------------------------------------------------------------
# 1. Tâche DBT : nettoyage initial
dbt_run_task = BashOperator(
    task_id="run_dbt_cleaning",
    bash_command="""
    source /home/ayoub/dbt-venv/bin/activate &&
    cd /home/ayoub/dbt_project/dbt_reviews &&
    dbt run
    """,
    dag=dag,
)

# ---------------------------------------------------------------------
# 2. Fonctions utilitaires améliorées

def improved_detect_language(text):
    """Détecte la langue parmi: français, anglais, espagnol, darija. Sinon: unknown."""
    
    if not isinstance(text, str) or not text.strip():
        return "unknown"
    
    # Liste des langues autorisées
    TARGET_LANGUAGES = {
        'fr': 'français',
        'en': 'anglais',
        'es': 'espagnol',
        'ar': 'darija'  # on considère 'ar' comme darija si indices darija présents
    }

    # Nettoyage simple du texte
    cleaned_text = ' '.join(text.strip().split())
    if len(cleaned_text) < 5:
        return "unknown"
    
    try:
        # Tentative de détection automatique
        detected_lang = detect(cleaned_text)

        # Si détectée automatiquement et autorisée
        if detected_lang in TARGET_LANGUAGES:
            return detected_lang

        # Sinon, détection manuelle pour Darija par mots-clés
        darija_indicators = ['bzzaf', 'hna', 'dyal', 'bzaf', 'wakha', 'ila', 'ghi', 'had', 'daba', 'khdam', 'khdma', 'bghit', 'kayn', 'machi', 'walo']
        text_lower = cleaned_text.lower()
        darija_count = sum(1 for word in darija_indicators if word in text_lower)
        if darija_count >= 2:
            return 'ar'  # considéré comme darija

        # Vérification de mots français ou anglais
        french_keywords = ['le', 'la', 'les', 'de', 'du', 'et', 'est', 'banque', 'service', 'client', 'très', 'bien', 'mal']
        english_keywords = ['the', 'is', 'are', 'with', 'bank', 'service', 'customer', 'good', 'bad', 'very', 'nice']

        fr_score = sum(1 for word in french_keywords if word in text_lower)
        en_score = sum(1 for word in english_keywords if word in text_lower)

        if fr_score >= 2 and fr_score > en_score:
            return 'fr'
        elif en_score >= 2:
            return 'en'

        return "unknown"

    except LangDetectException as e:
        logging.warning(f"LangDetect error: {e}")
        return "unknown"

def remove_stopwords_multilingual(text, language):
    """Supprime les stopwords selon la langue détectée"""
    if not isinstance(text, str) or not text.strip():
        return text
    
    try:
        # Mapping des codes de langue vers les langues NLTK
        lang_mapping = {
            'fr': 'french',
            'en': 'english',
            'es': 'spanish',
            'ar': 'arabic'  # pour darija, on utilise arabic
        }
        
        if language not in lang_mapping:
            return text
        
        nltk_lang = lang_mapping[language]
        
        # Vérifier si les stopwords sont disponibles
        try:
            stop_words = set(stopwords.words(nltk_lang))
        except OSError:
            logging.warning(f"Stopwords pour {nltk_lang} non disponibles")
            return text
        
        # Tokenization et suppression des stopwords
        tokens = word_tokenize(text.lower())
        
        # Supprimer les stopwords et conserver les mots significatifs
        filtered_tokens = [word for word in tokens if word not in stop_words and word.isalpha() and len(word) > 2]
        
        # Reconstituer le texte
        cleaned_text = ' '.join(filtered_tokens)
        
        # Retourner le texte original si le nettoyage a supprimé trop de contenu
        if len(cleaned_text) < len(text) * 0.3:  # Si moins de 30% du texte reste
            return text
        
        return cleaned_text if cleaned_text else text
        
    except Exception as e:
        logging.warning(f"Erreur suppression stopwords: {e}")
        return text

def get_bert_sentiment(text, sentiment_pipeline):
    """Analyse de sentiment avec BERT multilingue - optimisé pour 512 premiers caractères"""
    if not isinstance(text, str) or not text.strip():
        return "Neutral"
    
    try:
        # Prendre les 512 premiers caractères (optimisation BERT)
        text_truncated = text[:512]
        
        # Vérifier que le texte tronqué n'est pas trop court
        if len(text_truncated.strip()) < 10:
            return "Neutral"
        
        result = sentiment_pipeline(text_truncated)[0]
        label = result['label']
        confidence = result['score']
        
        # Mapping des labels BERT vers nos catégories avec seuil de confiance
        if confidence < 0.6:  # Seuil de confiance minimum
            return "Neutral"
        
        if "5" in label or "4" in label:
            return "Positive"
        elif "1" in label or "2" in label:
            return "Negative"
        else:
            return "Neutral"
            
    except Exception as e:
        logging.warning(f"Erreur analyse sentiment BERT: {e}")
        return "Neutral"

def check_column_exists(cursor, table_name, column_name):
    """Vérifier si une colonne existe"""
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s;
    """, (table_name, column_name))
    return len(cursor.fetchall()) > 0

# ---------------------------------------------------------------------
# Topics prédéfinis pour le domaine bancaire (mis à jour)
PREDEFINED_TOPICS = {
    'service_client': {
        'keywords': ['service', 'client', 'accueil', 'conseiller', 'aide', 'information', 'conseil', 'courtois', 'poli', 'aimable', 'serviable', 'competent'],
        'label': 'Service Client'
    },
    'personnel': {
        'keywords': ['personnel', 'employe', 'agent', 'staff', 'equipe', 'directeur', 'responsable', 'professionnel', 'qualifie', 'formation'],
        'label': 'Personnel'
    },
    'attente_file': {
        'keywords': ['attente', 'file', 'queue', 'patience', 'long', 'lent', 'rapide', 'vite', 'delai', 'temps', 'duree', 'rendez'],
        'label': 'Temps d\'attente'
    },
    'guichet': {
        'keywords': ['guichet', 'caisse', 'comptoir', 'depot', 'retrait', 'espece', 'cash', 'distributeur', 'dab', 'automate'],
        'label': 'Guichet/Caisse'
    },
    'telephone_digital': {
        'keywords': ['telephone', 'appel', 'tel', 'ligne', 'contact', 'joindre', 'repondre', 'decrocher', 'digital', 'application', 'app', 'site', 'internet', 'mobile'],
        'label': 'Téléphone/Digital'
    },
    'credit_pret': {
        'keywords': ['credit', 'pret', 'emprunt', 'financement', 'taux', 'interet', 'remboursement', 'mensualite', 'hypotheque', 'immobilier'],
        'label': 'Crédit/Prêt'
    },
    'compte_carte': {
        'keywords': ['compte', 'carte', 'visa', 'mastercard', 'bancaire', 'debit', 'solde', 'virement', 'transaction', 'decouvert'],
        'label': 'Compte/Carte'
    },
    'frais_tarifs': {
        'keywords': ['frais', 'tarif', 'cout', 'prix', 'cher', 'gratuit', 'commission', 'facturation', 'abonnement', 'economique'],
        'label': 'Frais/Tarifs'
    },
    'horaires_acces': {
        'keywords': ['horaire', 'ouvert', 'ferme', 'weekend', 'dimanche', 'access', 'parking', 'localisation', 'proche', 'agence'],
        'label': 'Horaires/Accès'
    },
    'probleme_technique': {
        'keywords': ['probleme', 'panne', 'erreur', 'bug', 'marche', 'fonctionne', 'technique', 'systeme', 'maintenance', 'dysfonctionnement'],
        'label': 'Problèmes techniques'
    }
}

def classify_review_topic(text):
    """Classification basée sur des topics prédéfinis pour le domaine bancaire"""
    if not isinstance(text, str) or not text.strip():
        return -1, "Unknown", "Unknown"
    
    text_lower = text.lower()
    # Nettoyage basique
    text_clean = re.sub(r'[^\w\s]', ' ', text_lower)
    words = text_clean.split()
    
    if len(words) == 0:
        return -1, "General", "avis general"
    
    topic_scores = {}
    
    # Calculer le score pour chaque topic
    for topic_key, topic_info in PREDEFINED_TOPICS.items():
        score = 0
        keywords = topic_info['keywords']
        
        # Compter les mots-clés présents
        for keyword in keywords:
            if keyword in text_clean:
                # Bonus si le mot est isolé (pas dans un autre mot)
                if f' {keyword} ' in f' {text_clean} ':
                    score += 2
                else:
                    score += 1
        
        # Normaliser par le nombre de mots dans le texte
        topic_scores[topic_key] = score / len(words) * 100
    
    # Trouver le topic avec le meilleur score
    if topic_scores:
        best_topic = max(topic_scores.items(), key=lambda x: x[1])
        
        # Seuil minimum pour assigner un topic
        if best_topic[1] > 0.5:  # Au moins 0.5% des mots sont des keywords
            topic_idx = list(PREDEFINED_TOPICS.keys()).index(best_topic[0])
            topic_info = PREDEFINED_TOPICS[best_topic[0]]
            return topic_idx, topic_info['label'], ', '.join(topic_info['keywords'][:5])
    
    return -1, "General", "avis general"

# ---------------------------------------------------------------------
# 3. Détection de langue avec nettoyage de stopwords
def enrich_language_and_clean_text():
    """Détection de langue et nettoyage des stopwords"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Charger les données nettoyées
        df = pd.read_sql("SELECT * FROM clean_reviews;", conn)
        logging.info(f"{len(df)} lignes chargées pour enrichissement.")

        if df.empty:
            logging.warning("Aucune donnée trouvée dans clean_reviews")
            return

        # Identifier la colonne de texte (review_text ou review)
        text_column = 'review_text' if 'review_text' in df.columns else 'review'
        if text_column not in df.columns:
            raise ValueError("Aucune colonne de texte trouvée (review_text ou review)")

        # Détection de langue améliorée
        logging.info("Début de la détection de langue...")
        df["language"] = df[text_column].apply(improved_detect_language)
        
        # Statistiques de langues
        lang_stats = df['language'].value_counts()
        logging.info(f"Langues détectées : {lang_stats.to_dict()}")

        # Nettoyage des stopwords selon la langue
        logging.info("Début du nettoyage des stopwords...")
        df["cleaned_text"] = df.apply(
            lambda row: remove_stopwords_multilingual(row[text_column], row['language']), 
            axis=1
        )
        
        # Statistiques après nettoyage
        avg_reduction = ((df[text_column].str.len() - df["cleaned_text"].str.len()) / df[text_column].str.len() * 100).mean()
        logging.info(f"Réduction moyenne du texte après suppression stopwords : {avg_reduction:.1f}%")

        # Créer la table enrichie avec texte nettoyé
        cursor.execute("DROP TABLE IF EXISTS clean_reviews_language;")
        
        # Copier la structure de clean_reviews et ajouter les nouvelles colonnes
        cursor.execute("""
            CREATE TABLE clean_reviews_language AS 
            SELECT *, 
                ''::VARCHAR(10) AS language,
                ''::TEXT AS cleaned_text
            FROM clean_reviews WHERE 1=0;
        """)
        conn.commit()

        # Insertion par batch
        columns = list(df.columns)
        for i in range(0, len(df), 1000):
            batch = df.iloc[i:i+1000]
            values = [tuple(x if pd.notna(x) else None for x in row) for row in batch.to_numpy()]
            
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"""
                INSERT INTO clean_reviews_language ({', '.join(columns)})
                VALUES ({placeholders})
            """
            cursor.executemany(insert_sql, values)
            conn.commit()

        logging.info(f"Table clean_reviews_language créée avec {len(df)} lignes.")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Erreur enrich_language_and_clean_text: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise

language_cleaning_task = PythonOperator(
    task_id="enrich_language_and_clean_text",
    python_callable=enrich_language_and_clean_text,
    dag=dag,
)

# ---------------------------------------------------------------------
# 4. Analyse de sentiment avec BERT optimisé
def analyze_sentiment_bert():
    """Analyse de sentiment avec BERT sur texte nettoyé"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Charger les avis avec langue et texte nettoyé
        df = pd.read_sql("""
            SELECT * FROM clean_reviews_language 
            WHERE language IN ('fr', 'en', 'es', 'ar') 
            AND cleaned_text IS NOT NULL 
            AND LENGTH(TRIM(cleaned_text)) >= 10;
        """, conn)
        
        logging.info(f"Nombre d'avis pour analyse de sentiment : {len(df)}")
        
        if df.empty:
            logging.warning("Aucun avis valide pour l'analyse de sentiment")
            return

        # Initialiser le pipeline de sentiment BERT
        logging.info("Initialisation du modèle BERT...")
        sentiment_pipeline = pipeline(
            'sentiment-analysis', 
            model='nlptown/bert-base-multilingual-uncased-sentiment',
            return_all_scores=False
        )

        # Analyse de sentiment par batch pour éviter les problèmes de mémoire
        logging.info("Début de l'analyse de sentiment sur texte nettoyé...")
        batch_size = 50
        sentiments = []
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch_sentiments = []
            
            for text in batch['cleaned_text']:
                sentiment = get_bert_sentiment(text, sentiment_pipeline)
                batch_sentiments.append(sentiment)
            
            sentiments.extend(batch_sentiments)
            logging.info(f"Batch {i//batch_size + 1}/{(len(df)-1)//batch_size + 1} traité")

        df['sentiment'] = sentiments
        
        # Statistiques de sentiment
        sentiment_stats = df['sentiment'].value_counts()
        logging.info(f"Répartition sentiment : {sentiment_stats.to_dict()}")

        # Ajouter la colonne sentiment à la table
        try:
            cursor.execute("ALTER TABLE clean_reviews_language ADD COLUMN sentiment VARCHAR(20);")
            conn.commit()
        except pg_errors.DuplicateColumn:
            conn.rollback()
            logging.info("Colonne sentiment existe déjà")

        # Mise à jour des sentiments
        if 'id' in df.columns:
            updates = [(row['sentiment'], row['id']) for _, row in df.iterrows()]
            
            if updates:
                execute_values(
                    cursor,
                    """UPDATE clean_reviews_language 
                       SET sentiment = data.sentiment
                       FROM (VALUES %s) AS data(sentiment, id) 
                       WHERE clean_reviews_language.id = data.id""",
                    updates,
                    page_size=1000
                )
                conn.commit()
                logging.info(f"Sentiments mis à jour pour {len(updates)} lignes")

        # Mettre à NULL le sentiment pour les avis non traités
        cursor.execute("""
            UPDATE clean_reviews_language 
            SET sentiment = 'Unknown' 
            WHERE sentiment IS NULL;
        """)
        conn.commit()

        logging.info("Analyse de sentiment terminée avec succès")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Erreur analyze_sentiment_bert: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise

sentiment_analysis_task = PythonOperator(
    task_id="analyze_sentiment_bert",
    python_callable=analyze_sentiment_bert,
    dag=dag,
)

# ---------------------------------------------------------------------
# 5. Classification de topics avec topics prédéfinis
def extract_topics_predefined():
    """Classification de topics avec topics prédéfinis pour le domaine bancaire"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Charger les avis avec sentiment
        df = pd.read_sql("""
            SELECT * FROM clean_reviews_language 
            WHERE language IN ('fr', 'en', 'es', 'ar') 
            AND sentiment != 'Unknown'
            AND cleaned_text IS NOT NULL;
        """, conn)
        
        logging.info(f"Nombre d'avis pour classification de topics : {len(df)}")
        
        if df.empty:
            logging.warning("Aucun avis valide pour la classification de topics")
            return

        # Classification des topics sur le texte nettoyé
        logging.info("Début de la classification de topics...")
        topics_data = []
        
        for idx, row in df.iterrows():
            # Utiliser le texte nettoyé pour la classification
            text = row['cleaned_text'] if row['cleaned_text'] else row.get('review_text', row.get('review', ''))
            topic_num, topic_label, topic_words = classify_review_topic(text)
            topics_data.append({
                'topic_number': topic_num,
                'topic_meaning': topic_label,
                'topic_words': topic_words
            })
        
        # Ajouter les colonnes au dataframe
        for key in ['topic_number', 'topic_meaning', 'topic_words']:
            df[key] = [item[key] for item in topics_data]
        
        # Statistiques des topics
        topic_stats = df['topic_meaning'].value_counts()
        logging.info("Distribution des topics :")
        for topic, count in topic_stats.items():
            logging.info(f"  {topic}: {count} avis")

        # Ajouter les colonnes topics à la table
        columns_to_add = [
            ("topic_number", "INTEGER"),
            ("topic_words", "TEXT"),
            ("topic_meaning", "TEXT")
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                cursor.execute(f"ALTER TABLE clean_reviews_language ADD COLUMN {col_name} {col_type};")
                conn.commit()
                logging.info(f"Colonne {col_name} ajoutée")
            except pg_errors.DuplicateColumn:
                conn.rollback()
                logging.info(f"Colonne {col_name} existe déjà")

        # Mise à jour par batch
        if 'id' in df.columns:
            updates = [
                (row['topic_number'], row['topic_words'], row['topic_meaning'], row['id']) 
                for _, row in df.iterrows()
            ]
            
            if updates:
                execute_values(
                    cursor,
                    """UPDATE clean_reviews_language 
                       SET topic_number = data.topic_number,
                           topic_words = data.topic_words,
                           topic_meaning = data.topic_meaning
                       FROM (VALUES %s) AS data(topic_number, topic_words, topic_meaning, id) 
                       WHERE clean_reviews_language.id = data.id""",
                    updates,
                    page_size=1000
                )
                conn.commit()
                logging.info(f"Topics mis à jour pour {len(updates)} lignes")

        # Créer la table finale
        cursor.execute("DROP TABLE IF EXISTS final_reviews;")
        cursor.execute("""
            CREATE TABLE final_reviews AS 
            SELECT * FROM clean_reviews_language;
        """)
        conn.commit()
        
        logging.info("Classification de topics terminée avec succès")
        
        # Afficher les topics trouvés
        final_topic_stats = df['topic_meaning'].value_counts()
        logging.info("=== TOPICS FINAUX IDENTIFIÉS ===")
        for topic, count in final_topic_stats.items():
            percentage = (count / len(df)) * 100
            logging.info(f"{topic}: {count} avis ({percentage:.1f}%)")
        
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Erreur extract_topics_predefined: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise

extract_topics_task = PythonOperator(
    task_id="extract_topics_predefined",
    python_callable=extract_topics_predefined,
    dag=dag,
)

# ---------------------------------------------------------------------
# 6. Tâche de validation et statistiques finales
def generate_final_stats():
    """Génère des statistiques finales sur les données enrichies"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        
        df = pd.read_sql("SELECT * FROM final_reviews;", conn)
        
        logging.info("=== STATISTIQUES FINALES ===")
        logging.info(f"Total des avis traités : {len(df)}")
        
        if 'language' in df.columns:
            lang_stats = df['language'].value_counts()
            logging.info(f"Langues détectées : {lang_stats.to_dict()}")
            
            # Pourcentages par langue
            for lang, count in lang_stats.items():
                pct = (count / len(df)) * 100
                lang_name = {
                    'fr': 'Français', 
                    'en': 'Anglais', 
                    'es': 'Espagnol', 
                    'ar': 'Darija/Arabe', 
                    'unknown': 'Inconnu'
                }.get(lang, lang)
                logging.info(f"  {lang_name}: {count} avis ({pct:.1f}%)")
        
        if 'sentiment' in df.columns:
            sentiment_stats = df['sentiment'].value_counts()
            logging.info(f"Sentiments :")
            for sentiment, count in sentiment_stats.items():
                pct = (count / len(df)) * 100
                logging.info(f"  {sentiment}: {count} avis ({pct:.1f}%)")
        
        if 'topic_meaning' in df.columns:
            topic_stats = df['topic_meaning'].value_counts()
            logging.info(f"Topics les plus fréquents :")
            for topic, count in topic_stats.head(10).items():
                pct = (count / len(df)) * 100
                logging.info(f"  {topic}: {count} avis ({pct:.1f}%)")
        
        # Analyse croisée sentiment x topic
        if 'sentiment' in df.columns and 'topic_meaning' in df.columns:
            cross_analysis = pd.crosstab(df['topic_meaning'], df['sentiment'])
            logging.info("=== ANALYSE CROISÉE TOPIC x SENTIMENT ===")
            logging.info(cross_analysis.to_string())
        
        # Statistiques sur le nettoyage de texte
        if 'cleaned_text' in df.columns:
            text_col = 'review_text' if 'review_text' in df.columns else 'review'
            if text_col in df.columns:
                original_lengths = df[text_col].str.len()
                cleaned_lengths = df['cleaned_text'].str.len()
                avg_reduction = ((original_lengths - cleaned_lengths) / original_lengths * 100).mean()
                logging.info(f"=== NETTOYAGE DE TEXTE ===")
                logging.info(f"Réduction moyenne après suppression stopwords : {avg_reduction:.1f}%")
        
        # Créer un résumé final dans une table de statistiques
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS pipeline_stats;")
        cursor.execute("""
            CREATE TABLE pipeline_stats AS
            SELECT 
                'total_reviews' AS metric, 
                COUNT(*)::TEXT AS value
            FROM final_reviews
            UNION ALL
            SELECT 
                'languages_detected' AS metric,
                COUNT(DISTINCT language)::TEXT AS value
            FROM final_reviews
            UNION ALL
            SELECT 
                'reviews_with_sentiment' AS metric,
                COUNT(*)::TEXT AS value
            FROM final_reviews 
            WHERE sentiment IS NOT NULL AND sentiment != 'Unknown'
            UNION ALL
            SELECT 
                'reviews_with_topics' AS metric,
                COUNT(*)::TEXT AS value
            FROM final_reviews 
            WHERE topic_meaning IS NOT NULL AND topic_meaning != 'Unknown';
        """)
        conn.commit()
        cursor.close()
        
        conn.close()
        logging.info("=== TRAITEMENT TERMINÉ AVEC SUCCÈS ===")
        
    except Exception as e:
        logging.error(f"Erreur generate_final_stats: {e}")
        raise

final_stats_task = PythonOperator(
    task_id="generate_final_stats",
    python_callable=generate_final_stats,
    dag=dag,
)

# ---------------------------------------------------------------------
# 7. Tâche de nettoyage et optimisation finale
def cleanup_and_optimize():
    """Nettoyage des tables intermédiaires et optimisation"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        logging.info("=== NETTOYAGE ET OPTIMISATION ===")
        
        # Créer des index pour améliorer les performances
        indexes = [
            ("final_reviews", "language"),
            ("final_reviews", "sentiment"),
            ("final_reviews", "topic_meaning"),
            ("final_reviews", "topic_number")
        ]
        
        for table, column in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column});")
                logging.info(f"Index créé sur {table}.{column}")
            except Exception as e:
                logging.warning(f"Erreur création index {table}.{column}: {e}")
        
        conn.commit()
        
        # Statistiques des tables
        cursor.execute("""
            SELECT 
                schemaname,
                tablename,
                attname,
                n_distinct,
                most_common_vals
            FROM pg_stats 
            WHERE tablename IN ('final_reviews', 'clean_reviews_language')
            ORDER BY tablename, attname;
        """)
        
        stats = cursor.fetchall()
        if stats:
            logging.info("=== STATISTIQUES DES COLONNES ===")
            for stat in stats:
                logging.info(f"Table: {stat[1]}, Colonne: {stat[2]}, Valeurs distinctes: {stat[3]}")
        
        # Analyser les tables pour mettre à jour les statistiques PostgreSQL
        cursor.execute("ANALYZE final_reviews;")
        cursor.execute("ANALYZE clean_reviews_language;")
        conn.commit()
        
        cursor.close()
        conn.close()
        logging.info("Nettoyage et optimisation terminés")
        
    except Exception as e:
        logging.error(f"Erreur cleanup_and_optimize: {e}")
        raise

cleanup_task = PythonOperator(
    task_id="cleanup_and_optimize",
    python_callable=cleanup_and_optimize,
    dag=dag,
)

# ---------------------------------------------------------------------
# 8. Tâche de génération de rapport final
def generate_final_report():
    """Génère un rapport final détaillé"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        
        # Requêtes pour le rapport
        queries = {
            "resume_general": """
                SELECT 
                    COUNT(*) as total_avis,
                    COUNT(CASE WHEN language != 'unknown' THEN 1 END) as avis_langue_detectee,
                    COUNT(CASE WHEN sentiment IS NOT NULL AND sentiment != 'Unknown' THEN 1 END) as avis_avec_sentiment,
                    COUNT(CASE WHEN topic_meaning IS NOT NULL AND topic_meaning != 'Unknown' THEN 1 END) as avis_avec_topic
                FROM final_reviews;
            """,
            
            "distribution_langues": """
                SELECT 
                    language,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
                FROM final_reviews 
                GROUP BY language 
                ORDER BY count DESC;
            """,
            
            "distribution_sentiments": """
                SELECT 
                    sentiment,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
                FROM final_reviews 
                WHERE sentiment IS NOT NULL
                GROUP BY sentiment 
                ORDER BY count DESC;
            """,
            
            "top_topics": """
                SELECT 
                    topic_meaning,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
                FROM final_reviews 
                WHERE topic_meaning IS NOT NULL AND topic_meaning != 'Unknown'
                GROUP BY topic_meaning 
                ORDER BY count DESC 
                LIMIT 10;
            """,
            
            "sentiment_par_langue": """
                SELECT 
                    language,
                    sentiment,
                    COUNT(*) as count
                FROM final_reviews 
                WHERE language != 'unknown' AND sentiment IS NOT NULL AND sentiment != 'Unknown'
                GROUP BY language, sentiment 
                ORDER BY language, count DESC;
            """,
            
            "topics_par_sentiment": """
                SELECT 
                    topic_meaning,
                    sentiment,
                    COUNT(*) as count
                FROM final_reviews 
                WHERE topic_meaning IS NOT NULL AND topic_meaning != 'Unknown'
                    AND sentiment IS NOT NULL AND sentiment != 'Unknown'
                GROUP BY topic_meaning, sentiment 
                ORDER BY topic_meaning, count DESC;
            """
        }
        
        logging.info("=== RAPPORT FINAL DÉTAILLÉ ===")
        
        for query_name, query in queries.items():
            logging.info(f"\n--- {query_name.upper().replace('_', ' ')} ---")
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                # Afficher les résultats de manière lisible
                for _, row in df.iterrows():
                    if query_name == "resume_general":
                        logging.info(f"Total des avis: {row['total_avis']}")
                        logging.info(f"Avis avec langue détectée: {row['avis_langue_detectee']}")
                        logging.info(f"Avis avec sentiment: {row['avis_avec_sentiment']}")
                        logging.info(f"Avis avec topic: {row['avis_avec_topic']}")
                    else:
                        row_str = " | ".join([f"{col}: {val}" for col, val in row.items()])
                        logging.info(f"  {row_str}")
            else:
                logging.info("  Aucune donnée trouvée")
        
        # Sauvegarder le rapport dans une table
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS final_report;")
        cursor.execute("""
            CREATE TABLE final_report (
                section VARCHAR(100),
                metric VARCHAR(100),
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insérer les métriques principales
        main_stats = pd.read_sql(queries["resume_general"], conn)
        if not main_stats.empty:
            stats_to_insert = [
                ("resume", "total_avis", str(main_stats.iloc[0]['total_avis'])),
                ("resume", "avis_langue_detectee", str(main_stats.iloc[0]['avis_langue_detectee'])),
                ("resume", "avis_avec_sentiment", str(main_stats.iloc[0]['avis_avec_sentiment'])),
                ("resume", "avis_avec_topic", str(main_stats.iloc[0]['avis_avec_topic']))
            ]
            
            cursor.executemany(
                "INSERT INTO final_report (section, metric, value) VALUES (%s, %s, %s)",
                stats_to_insert
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("\n=== PIPELINE TERMINÉ AVEC SUCCÈS ===")
        logging.info("Toutes les données sont disponibles dans la table 'final_reviews'")
        logging.info("Le rapport détaillé est dans la table 'final_report'")
        logging.info("Les statistiques du pipeline sont dans la table 'pipeline_stats'")
        
    except Exception as e:
        logging.error(f"Erreur generate_final_report: {e}")
        raise

final_report_task = PythonOperator(
    task_id="generate_final_report",
    python_callable=generate_final_report,
    dag=dag,
)

# ---------------------------------------------------------------------
# 9. Définir la séquence des tâches améliorée
dbt_run_task >> language_cleaning_task >> sentiment_analysis_task >> extract_topics_task >> final_stats_task >> cleanup_task >> final_report_task

# ---------------------------------------------------------------------
# 10. Configuration de monitoring et alertes
def check_data_quality():
    """Vérifie la qualité des données après traitement"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=BANK_CONN_ID)
        conn = postgres_hook.get_conn()
        
        # Vérifications de qualité
        quality_checks = {
            "pourcentage_avec_langue": """
                SELECT 
                    ROUND(COUNT(CASE WHEN language != 'unknown' THEN 1 END) * 100.0 / COUNT(*), 2) as pct
                FROM final_reviews;
            """,
            "pourcentage_avec_sentiment": """
                SELECT 
                    ROUND(COUNT(CASE WHEN sentiment IS NOT NULL AND sentiment != 'Unknown' THEN 1 END) * 100.0 / COUNT(*), 2) as pct
                FROM final_reviews;
            """,
            "pourcentage_avec_topic": """
                SELECT 
                    ROUND(COUNT(CASE WHEN topic_meaning IS NOT NULL AND topic_meaning != 'Unknown' THEN 1 END) * 100.0 / COUNT(*), 2) as pct
                FROM final_reviews;
            """
        }
        
        logging.info("=== CONTRÔLE QUALITÉ DES DONNÉES ===")
        
        for check_name, query in quality_checks.items():
            result = pd.read_sql(query, conn)
            percentage = result.iloc[0]['pct']
            
            # Seuils de qualité
            if check_name == "pourcentage_avec_langue" and percentage < 70:
                logging.warning(f"{check_name}: {percentage}% (seuil: 70%)")
            elif check_name == "pourcentage_avec_sentiment" and percentage < 60:
                logging.warning(f"{check_name}: {percentage}% (seuil: 60%)")
            elif check_name == "pourcentage_avec_topic" and percentage < 50:
                logging.warning(f"{check_name}: {percentage}% (seuil: 50%)")
            else:
                logging.info(f"{check_name}: {percentage}%")
        
        conn.close()
        
    except Exception as e:
        logging.error(f"Erreur check_data_quality: {e}")
        raise

quality_check_task = PythonOperator(
    task_id="check_data_quality",
    python_callable=check_data_quality,
    dag=dag,
)

# ---------------------------------------------------------------------
# Séquence finale des tâches
dbt_run_task >> language_cleaning_task >> sentiment_analysis_task >> extract_topics_task >> final_stats_task >> cleanup_task >> final_report_task >> quality_check_task