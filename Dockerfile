# specify start image
FROM python:3.10

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod 744 config.properties

CMD ["python3", "ARIMA/ForecastingScript.py"]