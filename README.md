# ArimaModule
Modulo python per lo sviluppo di un modello ARIMA con comunicazione verso MongoDB per prelevare 
i dati sui quali addestrare il modello e calcolare le predizioni che verranno salvate nello stesso DB.


### Testing 

Per poter confrontare i valori delle predizioni e, solo nel caso si abbia un congruo numero di
osservazioni è possibile far addestrare il modello rimuovendo dal train i dati che serviranno 
come test set.