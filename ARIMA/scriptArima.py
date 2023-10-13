from datetime import datetime, timedelta
from pmdarima import auto_arima

import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config.read('..\config.properties')

MEDIA_MOBILE_GIUDICE = config.get('JsonFields', 'media.mobile.giudice')
MEDIA_MOBILE_SEZIONE = config.get('JsonFields', 'media.mobile.sezione')

avg = []


def read_medie(lista_json, key):
    if key == "giudice":
        key = MEDIA_MOBILE_GIUDICE
    else:
        key = MEDIA_MOBILE_SEZIONE

    avg = [elemento[key] for elemento in lista_json]
    return avg


def create_json_with_prediction(prediction, lista_json, train_size, key, materia):
    nuova_lista_json = []
    data_prediction_unix = lista_json[train_size]["data_fine"]
    data_prediction = datetime.utcfromtimestamp(int(data_prediction_unix))

    for element in prediction:
        # Aggiungi 30 giorni alla data di previsione
        data_prediction = data_prediction + timedelta(days=30)
        # Converti la data di previsione in timestamp Unix
        data_prediction_unix = data_prediction.timestamp()
        print(data_prediction_unix)
        try:
            if materia is not None:
                if key == "giudice":
                    json_data = {
                        "giudice": lista_json[0]["giudice"],
                        "materia": materia,
                        "mediaMobileGiudice": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                    }
                else:
                    json_data = {
                        "ufficio": lista_json[0]["ufficio"],
                        "materia": materia,
                        "sezione": lista_json[0]["sezione"],
                        "mediaMobileSezione": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                    }
            else:
                if key == "giudice":
                    json_data = {
                        "giudice": lista_json[0]["giudice"],
                        "mediaMobileGiudice": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                    }
                else:
                    json_data = {
                        "ufficio": lista_json[0]["ufficio"],
                        "sezione": lista_json[0]["sezione"],
                        "mediaMobileSezione": element,
                        "data_fine": data_prediction_unix  # Aggiorna con il timestamp Unix
                    }

            nuova_lista_json.append(json_data)

        except Exception as e:
            print('Errore imprevisto: ', e.with_traceback())

    return nuova_lista_json


def predictions(key, lista, prediction_period, materia):
    avg = read_medie(lista, key)

    print("...Creazione del modello")
    # AUTO-arima_Model (Scelta automatica dei parametri del modello)
    arima_model = auto_arima(avg)
    arima_model.summary()

    prediction = arima_model.predict(n_periods=prediction_period)
    print("Calcolo completato.")

    nuova_lista_json = create_json_with_prediction(prediction, lista, (len(lista)-1), key, materia)
    print("Creazione json completata.")

    return nuova_lista_json
