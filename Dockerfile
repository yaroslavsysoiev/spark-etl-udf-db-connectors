FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar', '/opt/spark/jars/delta-spark_2.12-3.2.0.jar')"

RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar', '/opt/spark/jars/delta-storage-3.2.0.jar')"

RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar', '/opt/spark/jars/postgresql-42.7.8.jar')"

COPY . /app

WORKDIR /app

ENV PYTHONPATH=/app
