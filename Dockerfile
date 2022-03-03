FROM datamechanics/spark:3.0.1-latest

WORKDIR /opt/application/

COPY ./requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED 1
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

COPY landing_zone ./landing_zone
COPY src ./src
COPY main_etl.py main_services.py ./

CMD ["/bin/sh"]
