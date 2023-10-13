import json
import os

from confluent_kafka import *

import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config.read('..\config.properties')

TOPIC_GIUDICE = config.get('DestinationTopics', 'DESTINATION_TOPIC_GIUDICE')
TOPIC_SEZIONE = config.get('DestinationTopics', 'DESTINATION_TOPIC_SEZIONE')

DIR_FORECASTING_RESULTS = config.get('FilePathConfiguration', 'dir.forecasting.results')
FILE_FORECASTING_RESULTS = config.get('FilePathConfiguration', 'filename.forecasting')

topics = [TOPIC_GIUDICE, TOPIC_SEZIONE]
MIN_COMMIT_COUNT = 1

# auto.offset.reset legge a partire messaggio più vecchio disponibile sul topic non ancora letto"
conf = {
    'bootstrap.servers': config.get('Kafka', 'bootstrap.servers'),
    'group.id': config.get('Consumer', 'group.id'),
    'auto.offset.reset': config.get('Consumer', 'auto.offset.reset')
}


def shutdown():
    print("Chiusura in corso. Attendere...")
    consumer.close()
    print("Chiusura completata.")
    exit(0)


def salva_risultati_forecasting(key, dataReceived):
    for element in dataReceived:
        print(key)
        print(element)

        if not "materia" in element:
            if key == "sezione":
                fileDownload = FILE_FORECASTING_RESULTS + '_' + element["ufficio"] + '_' + key + '_' + element[key] + '.json'
            else:
                id = "".join(element[key].split())
                fileDownload = FILE_FORECASTING_RESULTS + '_' + key + '_' + id + '.json'
        else:
            if key == "sezione":
                fileDownload = FILE_FORECASTING_RESULTS + '_' + element["ufficio"] + '_' + key + '_' + element[key] + '_' + element["materia"] + '.json'
            else:
                id = "".join(element[key].split())
                fileDownload = FILE_FORECASTING_RESULTS + '_' + key + '_' + id + '_' + element["materia"] + '.json'

        percorso_completo = os.path.join(DIR_FORECASTING_RESULTS, fileDownload)
        opera_su_file(percorso_completo, element)


def opera_su_file(fileDownload, data):

    try:
        with open(fileDownload, 'r') as file:
            lista_json = json.load(file)
    except FileNotFoundError:
        # Se il file non esiste, crea una lista vuota
        lista_json = []

    if not any(element['data_fine'] == data['data_fine'] for element in lista_json):
        # Aggiunta del nuovo JSON alla lista
        lista_json.append(data)

    # Scrittura della lista aggiornata nel file
    with open(fileDownload, 'w') as file:
        json.dump(lista_json, file, indent=2)


def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            print('Received message by {}: {}'.format(msg.topic(), msg.value().decode('utf-8')))
            receivedData = json.loads(msg.value().decode('utf-8'))
            receivedByTopic = msg.topic()

            if receivedByTopic == TOPIC_GIUDICE:
                chiave = "giudice"
                salva_risultati_forecasting(chiave, receivedData)

            else:
                chiave = "sezione"
                salva_risultati_forecasting(chiave, receivedData)

            consumer.commit()

    except KeyboardInterrupt:
        shutdown()

    except Exception as e:
        print('Errore imprevisto: ', e.with_traceback())
        shutdown()


if __name__ == '__main__':

    if not os.path.exists(DIR_FORECASTING_RESULTS):
        os.makedirs(DIR_FORECASTING_RESULTS)

    consumer = Consumer(conf)
    consumer.subscribe(topics)
    while True:
        consume_loop(consumer, topics)
