# specify start image
FROM python:3.10

WORKDIR /ArimaModule_latest

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "ForecastingScript.py"]