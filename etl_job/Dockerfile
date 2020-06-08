
FROM python:3.6-slim

WORKDIR /etl_job

RUN pip install pymongo
RUN pip install SQLAlchemy
RUN pip install psycopg2-binary


CMD python etl_job.py
