import numpy as np
from matplotlib import pyplot as plt
from pmdarima import auto_arima
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_absolute_error
import os
import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.properties')
config.read(config_path)

PREDICTION_PERIOD = int(config.get('PredictionParams', 'prediction.period.mesi'))


def calcolo_metriche(train_test, test_set):
    try:
        errors = train_test - test_set
        errors_perc = np.abs((train_test - test_set) / train_test) * 100
        media_errori_percentuali = np.mean(errors_perc)
        mae = mean_absolute_error(train_test, test_set)
        print(errors)
        mse = np.mean(errors ** 2)
        rmse = np.sqrt(mse)

        with open('Metriche_performance.txt', 'a') as file:
            file.write(f"MSE: {mse} \t - \t")
            file.write(f"RMSE: {rmse} \t - \t")
            file.write(f"MAE: {mae:.10f} \t - \t")
            file.write(f"Errore medio (%): {media_errori_percentuali:.10f}%\n")
    except FileNotFoundError:
        # Se il file non esiste, crea una lista vuota
        print("Problema file per salvataggio RMSE")
    except Exception as e:
        print("Problema nel calcolo delle metriche: ", e)


def plotting(y1, y2, prediction_period):
    # Visualizzazione grafica della serie temporale
    x = np.linspace(0, prediction_period, prediction_period)
    plt.plot(x, y1, label='Dati reali di test', color='black', marker='o')

    # Aggiunta del secondo set di dati al plot esistente
    plt.plot(x, y2, label='Predizioni', color='red', marker='x')
    plt.title("Confronto dati reali - predizioni")
    plt.xlabel('Tempo')
    plt.ylabel('Media')
    plt.legend()
    plt.show()


def predictions(train_set, test_set):
    # Auto-ARIMA Model (Scelta automatica dei parametri del modello)
    try:
        # Con metodo di default lbfgs
        arima_model = auto_arima(train_set, trace=True, error_action='ignore', suppress_warnings=True)

        # Con metodo powell
        #arima_model = auto_arima(train_set, trace=True, error_action='ignore', suppress_warnings=True, method='powell')

        # Con metodo cg
        #arima_model = auto_arima(train_set, trace=True, error_action='ignore', suppress_warnings=True, method = 'cg')
        #prediction = arima_model.predict(n_periods = )
        # Con metodo ncg
        #arima_model = auto_arima(train_set, trace=True, error_action='ignore', suppress_warnings=True, method='ncg')

        # Con componente stagionale metodo di default lbfgs
        # arima_model = auto_arima(train_set, trace=True, seasonal=True, m=12, error_action='ignore', suppress_warnings=True)

        # Con componente stagionale e metodo cg
        # arima_model = auto_arima(train_set, trace=True, seasonal=True, m=12, error_action='ignore', suppress_warnings=True, method='cg')

        # Predizioni con predict_in_sample
        prediction = arima_model.predict_in_sample(start=len(train_set), end=len(train_set) + PREDICTION_PERIOD - 1)
        #prediction = arima_model.predict(n_periods=200)
        # Tentativo di ricostruzione nuovo modello statsmodel con parametri trovati dal tuning automatico di autoarima
        # e quindi utilizzo del metodo predict per ottenere le predizioni.
        # p, d, q = arima_model.order
        # # #ps, ds, qs, s = arima_model.seasonal_order
        # # # print(f"Seasonal order: {ps}, {ds}, {qs}, {s}")
        # # #model = ARIMA(train_set, order=(p, d, q), seasonal_order=(ps,ds,qs, s))
        # model = ARIMA(train_set, order=(p, d, q))
        # # Adatta il modello ai dati
        # fit_model = model.fit()
        # # Visualizza un riassunto del modello
        # print(fit_model.summary())
        # #Effettua previsioni con il modello
        # prediction = fit_model.predict(start=len(train_set), end=len(train_set) + len(test_set) - 1)

        test_set = np.array(test_set)
        prediction_test = np.array(prediction)
        # print("Avg last values: ", test_set)
        # print("Predictions", prediction_test)
        calcolo_metriche(test_set, prediction_test)
        plotting(test_set, prediction_test, PREDICTION_PERIOD)
        if not all(elemento == 0 for elemento in prediction_test):
            return prediction_test

    except Exception as e:
        print("Problema nella creazione del modello o nel calcolo delle metriche o nella produzione dei grafici: ", e)