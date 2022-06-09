FROM python:3.9-alpine

COPY . .
RUN pip3 install -r ./requrements.txt
ENTRYPOINT [ "python3", "./main.py" ]
