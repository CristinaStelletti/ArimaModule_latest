from pymongo import MongoClient
import json

file_path = ['../Data_for_db/download_dati_giudice_MinimoDellaPena.json',
             '../Data_for_db/download_dati_sezione_Avellino_01.json'
            ]


def inserimento_db():
    try:
        for filePath in file_path:
            with open(filePath, 'r') as file:
                lista_json = json.load(file)
                print(lista_json)
                collection.insert_many(lista_json)
                print("Ho inserito")

    except FileNotFoundError:
        # Se il file non esiste, crea una lista vuota
        print("Il file non esiste")
    except Exception as e:
        print("Eccezione generata, problema: {}".format(e.with_traceback()))


if __name__ == '__main__':

    client = MongoClient("mongodb://localhost:27017/")
    # Seleziona il database
    db = client["hyperion_MongoDB"]
    collection = db["medie_per_materia"]

    inserimento_db()