import gradio as gr
import requests
import json

# Funzione per inviare una domanda al tuo servizio AI
def query_service(question):
    url = "http://localhost:8000/question"  # Endpoint del tuo servizio
    payload = {"question": question}  # La domanda che invii come JSON
    try:
        # Invia la richiesta al servizio AI
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Solleva un'eccezione se la risposta è un errore HTTP
        data = response.json()  # Ottieni la risposta in formato JSON
        
        # Estrai i dati dalla risposta
        sql_query = data.get("sql_query", "Nessuna query SQL generata.")
        results = data.get("results", "[]")  # Assicurati che results sia una stringa JSON valida
        
        # Converti la stringa JSON in una lista di dizionari
        results_list = json.loads(results)  # Deserializza la stringa JSON
        
        # Se i risultati sono vuoti, restituisci un messaggio
        if not results_list:
            return sql_query, "Nessun risultato trovato."
        
        # Estrai le etichette delle colonne (chiavi del primo dizionario)
        column_labels = list(results_list[0].keys())
        
        # Aggiungi la numerazione delle righe come prima colonna
        table_data = [
            [index + 1] + list(result.values()) for index, result in enumerate(results_list)
        ]
        
        # Aggiungi il nome della colonna per la numerazione
        column_labels = ['#'] + column_labels
        
        # Restituisci la query SQL e la tabella (con intestazioni e dati)
        return sql_query, {"data": table_data, "headers": column_labels}
    
    except requests.exceptions.RequestException as e:
        return f"Errore durante la richiesta: {e}", {}

# Crea l'interfaccia Gradio
iface = gr.Interface(
    fn=query_service,  # La funzione da chiamare
    inputs=gr.Textbox(label="Fai una domanda"),  # Un campo di testo per l'input
    outputs=[gr.Textbox(label="SQL Query"), gr.Dataframe(label="Risultati")],  # Due output: uno per la query e uno per la tabella
    live=False  # La domanda viene inviata solo quando si preme il tasto "Invia"
)


# Avvia l'interfaccia
iface.launch()
