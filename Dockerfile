FROM python:slim

WORKDIR /usr/src/app

RUN apt update
RUN apt install git -y
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "landings.py"]