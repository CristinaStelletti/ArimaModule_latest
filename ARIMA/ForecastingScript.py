from datetime import datetime, timedelta

from pymongo import *
import configparser
import os
import ArimaScript

# Leggi il file di configurazione
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.properties')

try:
    config.read(config_path)
except Exception:
    print("Errore: impossibile leggere il file di configurazione!")

CLIENT = config.get('MongoDB', 'mongo.client')
DB_NAME = config.get('MongoDB', 'db.name')

MEDIA_MOBILE_GIUDICE = config.get('JsonFields', 'media.mobile.giudice.per.materia')

MEDIE_PER_MATERIA_COLLECTION_NAME = config.get('MongoDB', 'medie.per.materia.collection')
PREDIZIONI_COLLECTION_PER_MATERIA_GIUDICE = config.get('MongoDB', 'predizioni.per.materia.giudici.collection')

PERIODO_DI_PREDIZIONE = int(config.get('PredictionParams', 'prediction.period.mesi'))
PERIODO_DI_CAMPIONAMENTO = int(config.get('PredictionParams', 'periodo.di.campionamento.giorni'))


def extracting_average(lista_json):

    avg = [elemento[MEDIA_MOBILE_GIUDICE] for elemento in lista_json]
    print("AVG di {} su {}: {} ".format(lista_json[0]["giudice"], lista_json[0]["materia"], avg))
    # PER PROVARE CON N DATI IN MENO NELLA FASE DI TRAIN E CONFRONTARE LE PREDIZIONI
    avg_test = avg[-PERIODO_DI_PREDIZIONE:]
    avg = avg[:-PERIODO_DI_PREDIZIONE]
    return avg, avg_test


def create_json_with_prediction(prediction, lista, train_size, materia):
    nuova_lista_json = []
    try:
        data_prediction_unix = lista[train_size-1]["data_fine"]
        print("last_date: ", data_prediction_unix)
        data_prediction = datetime.utcfromtimestamp(int(data_prediction_unix))

        for element in prediction:
            # Aggiungi 30 giorni alla data di previsione
            data_prediction = data_prediction + timedelta(days=PERIODO_DI_CAMPIONAMENTO)
            # Converti la data di previsione in timestamp Unix
            data_prediction_unix = data_prediction.timestamp()

            json_data = {
                    "ufficio": lista[0]["ufficio"],
                    "giudice": lista[0]["giudice"],
                    "materia": materia,
                    "sezione": lista[0]["sezione"],
                    "mediaMobileGiudicePerMateria": element,
                    "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
            }

            nuova_lista_json.append(json_data)
    except Exception as e:
        print('Errore imprevisto in create_json_with_prediction: ', e)

    return nuova_lista_json

def computing_and_saving_predictions(documenti_ordinati, train_set, test_set, materia, predictionCollection):

    print(f"...Creazione del modello per {materia}")
    predictions = ArimaScript.predictions(train_set, test_set)
    if predictions is not None:
        try:
                nuova_lista_json = create_json_with_prediction(predictions, documenti_ordinati, len(train_set), materia)
                print("Creazione json completata.")
                # Controllo inesistenza del dato e scrittura nel DB
                for elem in nuova_lista_json:
                    query = {"giudice": elem["giudice"], "materia": materia, "data_fine": elem["data_fine"]}
                    cursor = predictionCollection.find(query)
                    documento = list(cursor)
                    if not documento:
                        predictionCollection.insert_one(elem)
                    else:
                        filtro = {"_id": documento[0]["_id"]}
                        # Istruzioni di aggiornamento
                        aggiornamento = {"$set": {MEDIA_MOBILE_GIUDICE:elem[MEDIA_MOBILE_GIUDICE]}}

                        # Aggiornamento del documento che corrisponde alla query
                        predictionCollection.update_one(filtro, aggiornamento)
        except Exception as e:
            print("Errore su creazione json o eventualmente sull'inserimento nel db: ", e)


def processing(collezione, predictionCollection):

    try:
        query = {"giudice": {"$exists": True}}

        cursor = collezione.find(query)

        giudici = cursor.distinct('giudice')
        for giudice in giudici:
            query = {'giudice': giudice}
            cursor = collezione.find(query)
            materie = cursor.distinct('materia')
            for materia in materie:
                query2 = {'materia': materia}
                combined_query = {'$and': [query, query2]}
                cursor = collezione.find(combined_query)
                documenti = list(cursor)
                sorted_json_list = sorted(documenti, key=lambda x: x["data_fine"])
                if len(sorted_json_list) > 5: # con 6 elementi fa la predizione
                    train_set, test_set = extracting_average(sorted_json_list)
                    computing_and_saving_predictions(sorted_json_list, train_set, test_set, materia, predictionCollection)
                else:
                    print("Non ci sono sufficienti medie per effettuare le predizioni")
    except Exception as e:
        print(f"Problema nell'estrazione dei documenti: {e}")


def db_connection():
    try:
        client = MongoClient(CLIENT)
        db = client[DB_NAME]
        medie_per_materia_collection = db[MEDIE_PER_MATERIA_COLLECTION_NAME]
        predizioni_collection_per_materia_giudice = db[PREDIZIONI_COLLECTION_PER_MATERIA_GIUDICE]

        return medie_per_materia_collection, predizioni_collection_per_materia_giudice

    except Exception as e:
        print(f"Error nella connessione al db: {e}")


if __name__ == '__main__':

    medie_collection, predizioni_collection = db_connection()
    print("Estrazione dati e filtraggio...")
    processing(medie_collection, predizioni_collection)