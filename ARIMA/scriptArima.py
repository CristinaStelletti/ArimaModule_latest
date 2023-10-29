from datetime import datetime, timedelta

import numpy as np
from matplotlib import pyplot as plt
from pmdarima import auto_arima
import statsmodels.api as sm
import os

import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.properties')
config.read(config_path)

MEDIA_MOBILE_GIUDICE = config.get('JsonFields', 'media.mobile.giudice')

avg = []

PERIODO_DI_CAMPIONAMENTO = int(config.get('PredictionParams', 'periodo.di.campionamento.giorni'))


def read_medie(lista_json):

    avg = [elemento[MEDIA_MOBILE_GIUDICE] for elemento in lista_json]
    #PER PROVARE CON 5 DATI IN MENO NELLA FASE DI TRAIN E CONFRONTARE LE PREDIZIONI
    #avg = avg[:-5]
    return avg


def create_json_with_prediction(prediction, lista_json, train_size, materia):
    nuova_lista_json = []
    #PER PROVARE CON 5 DATI IN MENO E CONFRONTARE LE PREDIZIONI
    #data_prediction_unix = lista_json[train_size-5]["data_fine"]
    data_prediction_unix = lista_json[train_size]["data_fine"]
    data_prediction = datetime.utcfromtimestamp(int(data_prediction_unix))

    for element in prediction:
        # Aggiungi 30 giorni alla data di previsione
        data_prediction = data_prediction + timedelta(days=PERIODO_DI_CAMPIONAMENTO)
        # Converti la data di previsione in timestamp Unix
        data_prediction_unix = data_prediction.timestamp()

        try:
            if materia is not None:
                json_data = {
                        "ufficio": lista_json[0]["ufficio"],
                        "giudice": lista_json[0]["giudice"],
                        "materia": materia,
                        "sezione": lista_json[0]["sezione"],
                        "mediaMobileGiudicePerMateria": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                }
            else:
                json_data = {
                        "ufficio": lista_json[0]["ufficio"],
                        "giudice": lista_json[0]["giudice"],
                        "sezione": lista_json[0]["sezione"],
                        "mediaMobileGiudicePerMateria": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                }

            nuova_lista_json.append(json_data)

        except Exception as e:
            print('Errore imprevisto: ', e.with_traceback())

    return nuova_lista_json


def predictions(key, lista, prediction_period, materia):

    sorted_json_list = sorted(lista, key=lambda x: x["data_fine"])
    print(sorted_json_list)
    avg = read_medie(sorted_json_list)
    print(avg)

    print("...Creazione del modello")
    # AUTO-arima_Model (Scelta automatica dei parametri del modello)
    try:
        print(len(avg))
        arima_model1 = auto_arima(avg, trace=True, error_action='ignore', suppress_warnings=True)
        arima_model1.fit(avg)
        prediction, conf_int = arima_model1.predict(n_periods=prediction_period, return_conf_int=True)
        print("Intervallo di confidenza: {}".format(conf_int))
        if not all(elemento == 0 for elemento in prediction):
            print("Calcolo completato.")
            nuova_lista_json = create_json_with_prediction(prediction, lista, (len(lista) - 1), materia)
            print("Creazione json completata.")
            return nuova_lista_json

    except Exception as e:
        return None

