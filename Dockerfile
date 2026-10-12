FROM spark:4.2.0-python3

USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar', '/opt/spark/jars/postgresql-42.7.8.jar')"
