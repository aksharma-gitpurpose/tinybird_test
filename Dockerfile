FROM python:3

WORKDIR /usr/src/app/tinybird

COPY requirements.txt ./ 
RUN pip install --no-cache-dir -r requirements.txt

COPY . . 




