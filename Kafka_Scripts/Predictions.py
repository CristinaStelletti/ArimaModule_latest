import time
from pymongo import *

from confluent_kafka import *
import json
import signal
import os

import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config.read('..\config.properties')

FILE_ARIMA = config.get('FilePathConfiguration', 'filename.modello')
DIR_DOWNLOAD_MODELLO = config.get('FilePathConfiguration', 'dir.download.modello')

CLIENT = config.get('MongoDB', 'mongo.client')
DB_NAME = config.get('MongoDB', 'db.name')
COLLECTION_NAME = config.get('MongoDB', 'collection.name')
COLLECTION_NAME1 = "medie_per_materia"

DESTINATION_TOPIC_GIUDICE = config.get('DestinationTopics', 'DESTINATION_TOPIC_GIUDICE')
DESTINATION_TOPIC_SEZIONE = config.get('DestinationTopics', 'DESTINATION_TOPIC_SEZIONE')

MAX_TENTATIVI = int(config.get('Kafka', 'max.attemps'))

PERIODO_DI_PREDIZIONE = int(config.get('PredictionParams', 'prediction.period'))  # mesi

keys = ['giudice', 'sezione']
liste = []

# Configurazione del produttore
producer_conf = {
    'bootstrap.servers': config.get('Kafka', 'bootstrap.servers'),
    'acks': config.get('Producer', 'acks')
}

producer = Producer(producer_conf)


# Funzione per la gestione dell'invio asincrono dei messaggi del produttore
def delivery_report(err, receivedMessage):
    if err is not None:
        print('Errore nella consegna del messaggio: {}'.format(err))
    else:
        print('Messaggio consegnato a {} [{}]'.format(receivedMessage.topic(), receivedMessage.partition()))


# Gestione di una chiusura pulita
def shutdown():
    print("Chiusura in corso. Attendere...")
    producer.flush()
    print("Chiusura completata.")
    exit(0)


# Collega la funzione di shutdown alla ricezione del segnale SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, lambda sig, frame: shutdown())


def computing(filePath, destinationTopic, dataReceived, key, periodoDiPredizione, materia):
    from ARIMA import scriptArima

    data = scriptArima.predictions(filePath, key, dataReceived, periodoDiPredizione, materia)
    # data = scriptArima.arima_predictions(filePath, key, dataReceived, periodoDiPredizione, materia)
    # Invia il messaggio elaborato al nuovo topic
    for tentativo in range(MAX_TENTATIVI):
        try:
            producer.produce(destinationTopic, value=json.dumps(data), callback=delivery_report)
            producer.flush()
            break
        except KafkaException as e:
            # Logga l'errore o gestiscilo in base alle esigenze
            print(f'Errore durante l\'invio del messaggio: {e}')
        except Exception as e:
            print(f'Tentativo {tentativo + 1} fallito. Attendo prima del prossimo tentativo.')
            time.sleep(2 ** tentativo)  # Ritardo crescente


def filtering_data(key, destination_topic, collezione, periodoDiPredizione):
    try:

        query = {key: {"$exists": True}}

        cursor = collezione.find(query)
        documenti = list(cursor)
        value = documenti[0][key]

        if key == "sezione":
            ufficio = documenti[0]["ufficio"]
            # ad esempio Benevento_Sezione_01_Appalto
            key2 = ufficio + '_' + key + '_' + value
            filePath = FILE_ARIMA + '_' + key2 + '.pkl'
        else:
            id = "".join(value.split())
            filePath = FILE_ARIMA + '_' + key + '_' + id + '.pkl'

        percorso_completo = os.path.join(DIR_DOWNLOAD_MODELLO, filePath)
        print(percorso_completo)
        computing(percorso_completo, destination_topic, documenti, key, periodoDiPredizione, None)

    except Exception as e:
        print("Exception occured: {}".format(e.with_traceback()))


def filtering_data_per_materia(key, destination_topic, collezione, periodoDiPredizione):
    try:

        query = {key: {"$exists": True}}

        cursor = collezione.find(query)
        documenti = list(cursor)
        value = documenti[0][key]

        materie = cursor.distinct('materia')

        for materia in materie:
            query2 = {'materia': materia}
            combined_query = {'$and': [query, query2]}
            cursor = collezione.find(combined_query)
            documenti = list(cursor)

            if key == "sezione":
                ufficio = documenti[0]["ufficio"]
                # ad esempio Benevento_Sezione_01_Appalto
                key2 = ufficio + '_' + key + '_' + value + '_' + materia
                filePath = FILE_ARIMA + '_' + key2 + '.pkl'
            else:
                id = "".join(value.split()) + '_' + materia
                filePath = FILE_ARIMA + '_' + key + '_' + id + '.pkl'

            percorso_completo = os.path.join(DIR_DOWNLOAD_MODELLO, filePath)
            computing(percorso_completo, destination_topic, documenti, key, periodoDiPredizione, materia)

    except Exception as e:
        print("Exception occured: {}".format(e.with_traceback()))


if __name__ == '__main__':

    if not os.path.exists(DIR_DOWNLOAD_MODELLO):
        os.makedirs(DIR_DOWNLOAD_MODELLO)

    client = MongoClient(CLIENT)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME1]
    collection2 = db[COLLECTION_NAME]

    try:
        filtering_data_per_materia(keys[0], DESTINATION_TOPIC_GIUDICE, collection, PERIODO_DI_PREDIZIONE)
        filtering_data_per_materia(keys[1], DESTINATION_TOPIC_SEZIONE, collection, PERIODO_DI_PREDIZIONE)
        filtering_data(keys[0], DESTINATION_TOPIC_GIUDICE, collection2, PERIODO_DI_PREDIZIONE)
        filtering_data(keys[1], DESTINATION_TOPIC_SEZIONE, collection2, PERIODO_DI_PREDIZIONE)

    except KeyboardInterrupt:
        shutdown()
    except Exception as e:
        print('Errore imprevisto: ', e.with_traceback())
        shutdown()
