import os
import sys
import logging
import requests
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import config



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
API_URL = "https://api-inference.huggingface.co/models/defog/sqlcoder-7b-2"
API_TOKEN = os.getenv("T2DB_HF_API_TOKEN", "your_huggingface_api_token")  # Inserisci la tua chiave API in una variabile di ambiente

headers = {"Authorization": f"Bearer {API_TOKEN}"}

def query_huggingface_api(prompt):
    """Esegue una richiesta al modello Hugging Face con il prompt specificato."""
    response = requests.post(API_URL, headers=headers, json={"inputs": prompt})
    if response.status_code == 200:
        return response.json()[0]['generated_text']
    else:
        logger.error(f"Errore nella richiesta API: {response.status_code} - {response.text}")
        raise Exception("Errore nella richiesta API")

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

def get_db_schema():
    """Recupera lo schema del database in formato SQL 'CREATE TABLE'."""
    try:
        inspector = inspect(engine)
        max_num_tables = 10

        schema_context = ""
        for table_name in inspector.get_table_names():
            if max_num_tables >= 0:            
                schema_context += f"CREATE TABLE {table_name} (\n"
                primary_keys = inspector.get_pk_constraint(table_name).get("constrained_columns", [])
                foreign_keys = inspector.get_foreign_keys(table_name)
                
                for column in inspector.get_columns(table_name):
                    column_name = column["name"]
                    column_type = column["type"]
                    is_primary = column_name in primary_keys
                    line = f"  {column_name} {column_type}"

                    if is_primary:
                        line += " PRIMARY KEY"
                    
                    line += ",\n"
                    schema_context += line
                
                schema_context = schema_context.rstrip(",\n") + "\n);\n\n"

                for fk in foreign_keys:
                    referenced_table = fk["referred_table"]
                    for local, remote in zip(fk["constrained_columns"], fk["referred_columns"]):
                        schema_context += f"-- {table_name}.{local} can be joined with {referenced_table}.{remote}\n"
            max_num_tables -= 1
        
        logger.info("Schema del database recuperato con successo.")
        logger.info(schema_context)
        return schema_context
    except Exception as e:
        logger.error(f"Errore nel recupero dello schema del database: {e}")
        raise

def question_to_sql(question, schema_context):
    """Genera una query SQL a partire dalla domanda e dal contesto dello schema."""
    try:
        logger.info(f"Generazione della query SQL per la domanda: '{question}' con schema fornito.")
        prompt = f"{schema_context}\nDomanda: {question}"
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
    try:
        logger.info("Inizio dello script. Scrivi 'exit' per terminare.")
        schema_context = get_db_schema()
        while True:
            question = input("Domanda: ")
            if question.lower() == "exit":
                logger.info("Uscita...")
                break
            try:
                sql_query = question_to_sql(question, schema_context)
                results = execute_query(sql_query)
                print("Risultati:")
                print(results)
            except Exception as e:
                logger.error(f"Errore durante la gestione della domanda '{question}': {e}")
                print(f"Errore: {e}")
    except KeyboardInterrupt:
        logger.info("Interruzione manuale dello script.")
    except Exception as e:
        logger.error(f"Errore inatteso nello script principale: {e}")

if __name__ == "__main__":
    main()
