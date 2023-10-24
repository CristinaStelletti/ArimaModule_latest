from datetime import datetime, timedelta
from pmdarima import auto_arima

import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config.read('..\config.properties')

MEDIA_MOBILE_GIUDICE = config.get('JsonFields', 'media.mobile.giudice')

avg = []

PERIODO_DI_CAMPIONAMENTO = int(config.get('PredictionParams', 'periodo.di.campionamento.giorni'))


def read_medie(lista_json):

    avg = [elemento[MEDIA_MOBILE_GIUDICE] for elemento in lista_json]
    return avg


def create_json_with_prediction(prediction, lista_json, train_size, materia):
    nuova_lista_json = []
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
                        "giudice": lista_json[0]["giudice"],
                        "materia": materia,
                        "mediaMobileGiudice": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                }
            else:
                json_data = {
                        "giudice": lista_json[0]["giudice"],
                        "mediaMobileGiudice": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                }

            nuova_lista_json.append(json_data)

        except Exception as e:
            print('Errore imprevisto: ', e.with_traceback())

    return nuova_lista_json


def predictions(key, lista, prediction_period, materia):
    avg = read_medie(lista)

    print("...Creazione del modello")
    # AUTO-arima_Model (Scelta automatica dei parametri del modello)
    arima_model = auto_arima(avg)
    arima_model.summary()

    prediction = arima_model.predict(n_periods=prediction_period)
    print("Calcolo completato.")

    nuova_lista_json = create_json_with_prediction(prediction, lista, (len(lista)-1), materia)
    print("Creazione json completata.")

    return nuova_lista_json
