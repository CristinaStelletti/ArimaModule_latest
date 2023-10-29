# ArimaModule_latest
Versione di Arima con comunicazione verso MongoDB per prelevare i dati sui quali 
addestrare il modello e calcolare le predizioni che verranno riscritte nello stesso DB.


### Testing 

Per poter confrontare i valori delle predizioni e, solo nel caso si abbia un congruo numero di
osservazioni è possibile far addestrare il modello rimuovendo dal train i dati che serviranno 
come validation set quindi andare nello script "scriptArima.py" alle righe 27 e 34 
(c'è il commento di suggerimento).
