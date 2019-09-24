FROM pypy:3

WORKDIR /usr/src/app

#do requirements before code because they change less
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir .

ENTRYPOINT ["ballgame"]

CMD ["ballgame", "input.csv"]