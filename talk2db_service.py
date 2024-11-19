import argparse
import os
import re
import sys
import logging
import requests
from sqlalchemy import create_engine, inspect, text
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import config
from openai import OpenAI

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI()

# Verifica le variabili di ambiente
required_env_vars = ["T2DB_DB_USER", "T2DB_DB_PASS", "T2DB_DB_HOST", "T2DB_DB_PORT", "T2DB_DB_NAME", "T2DB_HF_API_TOKEN"]

missing_vars = [var for var in required_env_vars if os.getenv(var) is None]

if missing_vars:
    logger.error(f"Errore: Le seguenti variabili di ambiente non sono impostate: {', '.join(missing_vars)}")
    sys.exit(1)
else:
    logger.info("Tutte le variabili di ambiente sono impostate correttamente.")

# Configura l'API di Hugging Face
API_URL = os.getenv('T2DB_HF_API_URL')
API_TOKEN = os.getenv("T2DB_HF_API_TOKEN")  # Inserisci la tua chiave API in una variabile di ambiente

headers = {"Authorization": f"Bearer {API_TOKEN}"}

class QuestionRequest(BaseModel):
    question: str

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
def query_openai_api(question, schema):
    try:
        client = OpenAI()
        client.api_key = os.environ.get('T2DB_OAI_API_TOKEN')

        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": """You are an expoert in TSQL. 
                 Your task is to translate the user's question into TSQL code that will run on SQLServer. 
                 Put aliases on table fields to avoid ambiguities, every filed in select clause must have an alias.
                 All the dates filed must be formatted as dd/mm/yyyy
                 Generate code for procedures execution declaring the variables for the procedures with the values passed in by the user and then
                 the call to the procedure.
                 Only respond with TSQL code, without any additional text, explanations, or comments."""},
                {
                    "role": "user",
                    "content": f"""Database Schema:
{schema}

Question: {question}
Translate the question above into TSQL code and provide only the TSQL statement. 
Put aliases on table fields to avoid ambiguities."""
                }
            ]
        )

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
    "__EFMigrationsHistory", "ANTENNE", "ANTENNE_LOG", "AspNetRoleClaims", "AspNetRoles", 
    "AspNetUserClaims", "AspNetUserLogins", "AspNetUserRoles", "AspNetUsers", "AspNetUserTokens", 
    "BATCH", "CLIENTI_LOG", "COEFFICIENTI_ANTENNE", "CustomCounters", "idtable", "ImportOpera", 
    "LOG_ERRORI_SAP", "LX2002_AVERAGED_MESSAGES", "LX2002MESSAGES", "MaintenanceActions", 
    "MaintenanceActionsUsedSpareParts", "MaintenanceDemographicActionTypes", "MaintenanceDemographicSpareParts", 
    "MaintenanceDemographicSuppliers", "MaintenanceDemographicTargetItemProperties", 
    "MaintenanceDemographicTargetItems", "MaintenanceDemographicTargetTypeProperties", 
    "MaintenanceDemographicTargetTypes", "MaintenanceDemographicUnitOfMeasure", "MAP_INFO", "MENU", 
    "MENUOPZIONI", "MOVIMBARC_USCITE_GENERATE_LOG", "NW_RILEVAZIONI_GIORNALIERE", "PARAMETRIGENERALI", 
    "PN_ContiAttivi", "risorsebak", "STAMPE_IMMAGINI", "STAMPE_RENDLETT", "TRANSP_POSIZIONI", 
    "TRANSP_POSIZIONI_LOG", "TRANSPONDER_WEIGHTS", "TRANSPONDERS", "VERSIONIPATCHDB"
]

def get_db_procedures():
    """Estrae tutte le stored procedures e i loro parametri, rimuovendo '@' dai parametri e includendo i loro tipi."""
    try:
        # Query per estrarre le stored procedures
        query_procs = """
            SELECT SPECIFIC_NAME 
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
        """
        
        # Esegui la query
        with engine.connect() as connection:
            result = connection.execute(text(query_procs))
            stored_procs = result.fetchall()
        
        procedures_list = []
        
        # Per ogni stored procedure, recupera i parametri
        for proc in stored_procs:
            proc_name = proc[0]
            
            # Query per ottenere i parametri e i tipi della stored procedure
            query_params = f"""
                SELECT PARAMETER_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.PARAMETERS
                WHERE SPECIFIC_NAME = '{proc_name}'
            """
            
            # Esegui la query dei parametri e dei tipi
            with engine.connect() as connection:
                result_params = connection.execute(text(query_params))
                parameters = result_params.fetchall()
            
            # Rimuovi '@' dai parametri e includi il tipo
            param_list = [f"{param[0].replace('@', '')} {param[1]}" for param in parameters]
            formatted_proc = f"{proc_name}({', '.join(param_list)})" if param_list else f"{proc_name}()"
            procedures_list.append(formatted_proc)
        
        return procedures_list

    except Exception as e:
        logger.error(f"Errore nell'estrazione delle stored procedures: {e}")
        raise



def get_db_schema():
    """Recupera lo schema del database in formato compatto."""
    try:
        inspector = inspect(engine)
        schema_context = ""

        table_names = inspector.get_table_names()

        for table_name in table_names:
             if table_name not in filtered_tables:
                columns = inspector.get_columns(table_name)
                column_names = [column["name"] for column in columns]
                schema_context += f"{table_name} ({', '.join(column_names)})\n"
        
        procedures_list = get_db_procedures()

        schema_context += '\n'.join(procedures_list)

        # print(schema_context)
        return schema_context
    except Exception as e:
        logger.error(f"Errore nel recupero dello schema del database: {e}")
        raise

def question_to_sql(question, schema_context, use_openai):
    """Genera una query SQL a partire dalla domanda e dal contesto dello schema."""
    try:
        logger.info(f"Generazione della query SQL per la domanda: '{question}' con schema fornito.")
        prompt = create_sql_prompt(question, schema_context)
        
        sql_query = ""
        if use_openai:
            sql_query = query_openai_api(question, schema_context)
        else:
            sql_query = query_huggingface_api(prompt)


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


def save_dataframe_to_file(df, filename="output.csv"):
    """Salva il DataFrame in un file CSV."""
    try:
        df.to_csv(filename, index=False)
        logger.info(f"File salvato correttamente in {filename}.")
    except Exception as e:
        logger.error(f"Errore durante il salvataggio del file: {e}")
        raise


@app.post("/question")
async def process_question(request: QuestionRequest):
    """Elaborazione della domanda dell'utente e restituzione dei risultati."""
    try:
        question = request.question
        schema_context = get_db_schema()
        
        # Eseguiamo la generazione della query SQL
        use_openai = True  # Puoi passare True se vuoi usare OpenAI
        sql_query = question_to_sql(question, schema_context, use_openai)

        print(f"query generata: {sql_query}")

        # Eseguiamo la query e otteniamo i risultati
        results_df = execute_query(sql_query)

        results_df = results_df.fillna("")
        # Rendi uniche le colonne duplicate
        results_df.columns = [f"{col}_{i}" if results_df.columns.tolist().count(col) > 1 else col for i, col in enumerate(results_df.columns)]


        # save_dataframe_to_file(results_df)
        # return 

        # Converte i risultati in una lista di dizionari
        json_result = results_df.to_json(orient='records') 

      

        # Ritorniamo i risultati in formato JSON
        response = {"sql_query": sql_query, "results": json_result}

        print(response)
        return response
    
    except Exception as e:
        logger.error(f"Errore durante il processo della domanda '{request.question}': {e}")
        raise HTTPException(status_code=500, detail=f"Errore: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
