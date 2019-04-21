FROM jandaluz/pubg-agg:latest

WORKDIR /usr/src/app

RUN pip install --no-cache-dir google-cloud-storage==1.15.0 google-cloud-pubsub==0.40.0
COPY . .

CMD [ "python", "main_gcp.py"]