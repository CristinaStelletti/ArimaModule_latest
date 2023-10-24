from pymongo import *
import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config.read('..\config.properties')

CLIENT = config.get('MongoDB', 'mongo.client')
DB_NAME = config.get('MongoDB', 'db.name')

MEDIA_MOBILE_GIUDICE = config.get('JsonFields', 'media.mobile.giudice')

MEDIE_COLLECTION_NAME = config.get('MongoDB', 'medie.collection.name')
MEDIE_PER_MATERIA_COLLECTION_NAME = config.get('MongoDB', 'medie.per.materia.collection')
PREDIZIONI_COLLECTION_GIUDICE_NAME = config.get('MongoDB', 'predizioni.giudici.collection')
PREDIZIONI_COLLECTION_PER_MATERIA_GIUDICE = config.get('MongoDB', 'predizioni.per.materia.giudici.collection')

PERIODO_DI_PREDIZIONE = int(config.get('PredictionParams', 'prediction.period.mesi'))


def computing(dataReceived, key, periodoDiPredizione, materia, predictionCollection, campo):
    from ARIMA import scriptArima

    data = scriptArima.predictions(key, dataReceived, periodoDiPredizione, materia)

    # Controllo inesistenza del dato e scrittura nel DB
    for elem in data:
        query = {key: elem[key], "materia": materia, "data_fine": elem["data_fine"]}
        cursor = predictionCollection.find(query)
        documento = list(cursor)
        if not documento:
            predictionCollection.insert_one(elem)
        else:
            filtro = {"_id": documento[0]["_id"]}
            # Istruzioni di aggiornamento
            aggiornamento = {"$set": {campo:elem[campo]}}

            # Aggiornamento del documento che corrisponde alla query
            predictionCollection.update_one(filtro, aggiornamento)


def filtering_data(key, collezione, periodoDiPredizione, predictionCollection, campo):
    try:

        query = {key: {"$exists": True}}

        cursor = collezione.find(query)
        documenti = list(cursor)

        computing(documenti, key, periodoDiPredizione, None, predictionCollection, campo)

    except Exception as exc:
        print("Exception occured: {}".format(exc.with_traceback()))


def filtering_data_per_materia(key, collezione, periodoDiPredizione, predictionCollection, campo):
    try:

        query = {key: {"$exists": True}}

        cursor = collezione.find(query)

        materie = cursor.distinct('materia')

        for materia in materie:
            query2 = {'materia': materia}
            combined_query = {'$and': [query, query2]}
            cursor = collezione.find(combined_query)
            documenti = list(cursor)

            computing(documenti, key, periodoDiPredizione, materia, predictionCollection, campo)

    except Exception as exc:
        print("Exception occured: {}".format(exc.with_traceback()))


def lettura_dati(predizioniCollection, key, denominazione, materia):

    #query = {key: denominazione, "materia": materia}

    cursor = predizioniCollection.find()
    docs = list(cursor)
    print("DOCS: ")
    for doc in docs:
        print(doc)


if __name__ == '__main__':

    client = MongoClient(CLIENT)
    db = client[DB_NAME]
    medie_per_materia_collection = db[MEDIE_PER_MATERIA_COLLECTION_NAME]
    medie_collection = db[MEDIE_COLLECTION_NAME]
    predizioni_collection_giudice = db[PREDIZIONI_COLLECTION_GIUDICE_NAME]
    predizioni_collection_per_materia_giudice = db[PREDIZIONI_COLLECTION_PER_MATERIA_GIUDICE]

    try:
        # print("Estrazione dati e filtraggio...")
        # filtering_data_per_materia("giudice", medie_per_materia_collection, PERIODO_DI_PREDIZIONE, predizioni_collection_per_materia_giudice, "mediaMobileGiudice")
        # filtering_data("giudice", medie_collection, PERIODO_DI_PREDIZIONE, predizioni_collection_giudice, MEDIA_MOBILE_GIUDICE)

        lettura_dati(predizioni_collection_giudice, "giudice", "Minimo della Pena", "Appalto")
        lettura_dati(predizioni_collection_per_materia_giudice, "giudice", "Minimo della Pena", "Appalto")

    except Exception as exc:
        print("Exception traceback: {}".format(exc.with_traceback()))
