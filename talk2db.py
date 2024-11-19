import argparse
import os
import re
import sys
import logging
import requests
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import config
from openai import OpenAI

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Verifica le variabili di ambiente
required_env_vars = ["T2DB_DB_USER", "T2DB_DB_PASS", "T2DB_DB_HOST", "T2DB_DB_PORT", "T2DB_DB_NAME", "T2DB_HF_API_TOKEN"]

missing_vars = [var for var in required_env_vars if os.getenv(var) is None]

if missing_vars:
    logger.error(f"Errore: Le seguenti variabili di ambiente non sono impostate: {', '.join(missing_vars)}")
    sys.exit(1)
else:
    logger.info("Tutte le variabili di ambiente sono impostate correttamente.")
    # Stampa le variabili per verifica


# Configura l'API di Hugging Face
API_URL = os.getenv('T2DB_HF_API_URL')
API_TOKEN = os.getenv("T2DB_HF_API_TOKEN")  # Inserisci la tua chiave API in una variabile di ambiente

headers = {"Authorization": f"Bearer {API_TOKEN}"}

def create_sql_prompt(user_question, db_schema):
    prompt = f"""[INST] <<SYS>>
You are a helpful assistant specialized in generating SQL queries. Your task is to translate the user's question into SQL code that will run on a database. Only respond with SQL code, without any additional text, explanations, or comments.
<</SYS>>
Database Schema:
{db_schema}

Question: {user_question}
Translate the question above into TSQL code and provide only the SQL statement.
[/INST]"""
    return prompt

def query_huggingface_api(prompt):
    """Esegue una richiesta al modello Hugging Face con il prompt specificato."""
    response = requests.post(API_URL, headers=headers, json={"inputs": prompt})
    logger.debug(response)
    if response.status_code == 200:
        return response.json()[0]['generated_text']
    else:
        logger.error(f"Errore nella richiesta API: {response.status_code} - {response.text}")
        raise Exception("Errore nella richiesta API")
# Funzione per chiamare l'API di OpenAI
def query_openai_api(question,schema):
    
    try:
        client = OpenAI()
        client.api_key = os.environ.get('T2DB_OAI_API_TOKEN')

        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant specialized in generating SQL queries. Your task is to translate the user's question into TSQL code that will run on a database. Only respond with SQL code, without any additional text, explanations, or comments."},
                {
                    "role": "user",
                    "content": f"""Database Schema:
{schema}

Question: {question}
Translate the question above into SQL code and provide only the SQL statement."""
                }
            ]
        )
        # print(completion)

        sql_code = re.sub(r"^```sql\n|```$", "", completion.choices[0].message.content).strip()
        return sql_code

    except Exception as e:
        logger.error(f"Errore nella richiesta API OpenAI: {e}")
        raise

# Configura la connessione al database usando variabili di ambiente
DB_USER = os.getenv("T2DB_DB_USER")
DB_PASS = os.getenv("T2DB_DB_PASS")
DB_HOST = os.getenv("T2DB_DB_HOST")
DB_PORT = os.getenv("T2DB_DB_PORT", "1433")
DB_NAME = os.getenv("T2DB_DB_NAME")

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    logger.error("Errore: Assicurati che tutte le variabili di ambiente per la connessione al database siano impostate.")
    sys.exit(1)

connection_url = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Encrypt=no"

try:
    logger.info("Connessione al database...")
    engine = create_engine(connection_url)
    logger.info("Connessione al database stabilita.")
except Exception as e:
    logger.error(f"Errore nella connessione al database: {e}")
    sys.exit(1)
    
filtered_tables = [
    "__EFMigrationsHistory",
    "ANTENNE",
    "ANTENNE_LOG",
    "AspNetRoleClaims",
    "AspNetRoles",
    "AspNetUserClaims",
    "AspNetUserLogins",
    "AspNetUserRoles",
    "AspNetUsers",
    "AspNetUserTokens",
    "BATCH",
    "CLIENTI_LOG",
    "COEFFICIENTI_ANTENNE",
    "CustomCounters",
    "idtable",
    "ImportOpera",
    "LOG_ERRORI_SAP",
    "LX2002_AVERAGED_MESSAGES",
    "LX2002MESSAGES",
    "MaintenanceActions",
    "MaintenanceActionsUsedSpareParts",
    "MaintenanceDemographicActionTypes",
    "MaintenanceDemographicSpareParts",
    "MaintenanceDemographicSuppliers",
    "MaintenanceDemographicTargetItemProperties",
    "MaintenanceDemographicTargetItems",
    "MaintenanceDemographicTargetTypeProperties",
    "MaintenanceDemographicTargetTypes",
    "MaintenanceDemographicUnitOfMeasure",
    "MAP_INFO",
    "MENU",
    "MENUOPZIONI",
    "MOVIMBARC_USCITE_GENERATE_LOG",
    "NW_RILEVAZIONI_GIORNALIERE",
    "PARAMETRIGENERALI",
    "PN_ContiAttivi",
    "risorsebak",
    "STAMPE_IMMAGINI",
    "STAMPE_RENDLETT",
    "TRANSP_POSIZIONI",
    "TRANSP_POSIZIONI_LOG",
    "TRANSPONDER_WEIGHTS",
    "TRANSPONDERS",
    "VERSIONIPATCHDB"
]

def get_db_schema():
    """Recupera lo schema del database in formato compatto."""
    try:
        inspector = inspect(engine)
        schema_context = ""

        table_names = inspector.get_table_names()

        # print(",".join(table_names))

        for table_name in table_names:
             if table_name not in filtered_tables:
                columns = inspector.get_columns(table_name)
                column_names = [column["name"] for column in columns]
                schema_context += f"{table_name} ({', '.join(column_names)})\n"
        
        # logger.info("Schema del database recuperato con successo.")
        # logger.info(schema_context)
        return schema_context
    except Exception as e:
        logger.error(f"Errore nel recupero dello schema del database: {e}")
        raise



def question_to_sql(question, schema_context,use_openai):
    """Genera una query SQL a partire dalla domanda e dal contesto dello schema."""
    try:
        logger.info(f"Generazione della query SQL per la domanda: '{question}' con schema fornito.")
        prompt = create_sql_prompt(question, schema_context)
        
        # print(prompt)
        # return

        sql_query =""
        if(use_openai == True):
            sql_query = query_openai_api(prompt,schema_context)
        else:
            sql_query = query_huggingface_api(prompt)

        logger.info(f"Query SQL generata: {sql_query}")
        return sql_query
    except Exception as e:
        logger.error(f"Errore nella generazione della query SQL: {e}")
        raise

def execute_query(query):
    """Esegue la query SQL e restituisce i risultati come DataFrame."""
    try:
        with engine.connect() as connection:
            logger.info(f"Esecuzione della query: {query}")
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info("Query eseguita con successo.")
            return df
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {e}")
        raise

def main():
    """Funzione principale dello script."""
    parser = argparse.ArgumentParser(description="Genera query SQL usando OpenAI o Hugging Face")
    parser.add_argument("--oai", action="store_true", help="Usa l'API di OpenAI")
    parser.add_argument("--hf", action="store_true", help="Usa l'API di Hugging Face")
    args = parser.parse_args()

    # Controllo dei parametri
    if not args.oai and not args.hf:
        logger.error("Errore: Specificare --openai o --hf per scegliere l'API.")
        sys.exit(1)
    use_openai = args.oai


    try:
        logger.info("Inizio dello script. Scrivi 'exit' per terminare.")
        schema_context = get_db_schema()
        while True:
            question = input("Domanda: ")
            if question.lower() == "exit":
                logger.info("Uscita...")
                break
            try:
                sql_query = question_to_sql(question, schema_context,use_openai)
                logger.info(f"Query genrata:\n {sql_query}")
                # return
                results = execute_query(sql_query)
                print("Risultati:\n")
                print("--------------------------------")
                print(results)
                print("--------------------------------")
            except Exception as e:
                logger.error(f"Errore durante la gestione della domanda '{question}': {e}")
                print(f"Errore: {e}")
    except KeyboardInterrupt:
        logger.info("Interruzione manuale dello script.")
    except Exception as e:
        logger.error(f"Errore inatteso nello script principale: {e}")

if __name__ == "__main__":
    main()
