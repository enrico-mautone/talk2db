from sqlalchemy import create_engine, inspect
from datetime import datetime

# Configura la connessione al database
DB_USER = "sa"
DB_PASS = "Password01!"
DB_PORT = "1433"
DB_NAME = "NAUS_PROD"
DB_HOST = "WINDELL-186CUHK\\SQLEXPRESS"
connection_url = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Encrypt=no"
# Crea l'engine di SQLAlchemy
engine = create_engine(connection_url)

# Crea l'inspector per ispezionare le tabelle
inspector = inspect(engine)

# Ottieni tutte le tabelle nel database
tables = inspector.get_table_names()

# Prepara il nome del file con timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"schema_{timestamp}.sql"

# Apri il file per scrivere lo schema
with open(filename, "w") as file:
    for table_name in tables:
        # Ottieni i dettagli delle colonne della tabella
        columns = inspector.get_columns(table_name)
        
        # Costruisci la definizione della tabella
        table_definition = f"CREATE TABLE {table_name} (\n"
        column_definitions = []
        
        for column in columns:
            # Estrai il tipo e i dettagli della colonna
            column_name = column["name"]
            column_type = column["type"]
            is_nullable = column["nullable"]
            default = column.get("default", None)
            
            # Definisci il tipo di colonna e le restrizioni
            column_def = f"    {column_name} {column_type}"
            if not is_nullable:
                column_def += " NOT NULL"
            if default is not None:
                column_def += f" DEFAULT {default}"
                
            column_definitions.append(column_def)
        
        table_definition += ",\n".join(column_definitions) + "\n);\n\n"
        
        # Scrivi la definizione della tabella nel file
        file.write(table_definition)

print(f"Schema salvato in {filename}")
