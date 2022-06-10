FROM python:3.9-alpine

COPY . .
RUN pip3 install -r ./requirements.txt
ENV TZ="Asia/Seoul"

ENTRYPOINT [ "python3", "./main.py" ]
