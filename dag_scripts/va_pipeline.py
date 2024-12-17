import requests
from datetime import datetime 
import pandas as pd 
import os, subprocess 
import zipfile 
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.operators.bash import BashOperator 
from airflow.utils.dates import days_ago 

#initialize dag args 
default_args = {
    'startdate': datetime(2024,12,10),
}

dag = Dag(
    dag_id = 'vawater_pipeline',
    default_args = default_args,
    start_date = days_ago(0),
    schedule_interval=None,
)
#water data extract and transform task
waterdt_ET = DockerOperator(
    task_id="waterdata_extract",
    image="water-etl-app",
    command="--state 'virginia' --startyear '2020' --endyear '2024'",
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    mounts=['/home/kb/dockerfiles/dockerfiles_output','/app/output'],
    auto_remove=True,
    dag=dag
)

#prep files for loading
postgres_prep = BashOperator(
    task_id="postgres_prep",
    bash_command= 'mv /home/kb/dockerfiles/dockerfiles_output/*.csv /var/lib/postgresql/',
    dag=dag,
)

#load data to postgresdb
waterdt_Load = PostgresOperator(
    task_id="waterdata_load",
    postgres_conn_id="va_conn",
    sql = """
        COPY result (resultidentifier, resultdetectionconditiontext, resultsamplefractiontext, resultmeasurevalue, resultmeasureunitcode, resultstatusidentifier, resultvaluetypename, resultweightbasistext, resulttimebasistext, resulttemperaturebasistext, resultparticlesizebasistext, resultcommenttext, resultdepthheightmeasurevalue, resultdepthheightmeasureunitcode, resultdepthaltitudereferencepointtext, resultfileurl, resultanalyticalmethodidentifier, resultanalyticalmethodname, resultanalyticalmethoddescriptiontext,resultdetectionlimiturl,resultlaboratorycommenttext) FROM '/var/lib/postgresql/resulttbl.csv' WITH (FORMAT CSV, HEADER);

        COPY activity (activityidentifier, activitytypecode, activitymedianame, activitymediasubdivisionname, activitystartdate, activityenddate, activityrelativedepthname, activitydepthheightmeasurevalue, activitydepthaltitudereferencepointtext, activitytopdepthheightmeasurevalue, activitytopdepthheightmeasureunitcode, activitybottomdepthheightmeasurevalue, activitycommenttext,resultid) FROM '/var/lib/postgresql/activitytbl.csv' WITH (FORMAT CSV, HEADER);

        COPY charactersitic (characteristicname, hydrologicevent, hydrologiccondition,resultid) FROM '/var/lib/postgresql/characteristictbl.csv' WITH (FORMAT CSV, HEADER);

        COPY detectionlimit (detectionlimittypename, detectionlimitmeasurevalue, detectionlimitmeasureunitcode,resultid) FROM '/var/lib/postgresql/detectionlimittbl.csv' WITH (FORMAT CSV, HEADER);
         
        COPY location (monitoringlocationname, monitoringlocationid, latitude, longitude,resultid) FROM '/var/lib/postgresql/locationtbl.csv' WITH (FORMAT CSV, HEADER);

        COPY organization (organizationformalname, organizationalidentifier,resultid) FROM '/var/lib/postgresql/organizationtbl.csv' WITH (FORMAT CSV, HEADER);

        COPY sample (samplecollectionmethodid, samplecollectionmethodidcontext, samplecollectionmethodname, samplecollectionmethoddescriptiontext, samplecollectionequipmentname, sampletissueanatomyname, sampleaquifer, lamsampleurl,resultid) FROM '/var/lib/postgresql/sampletbl.csv' WITH (FORMAT CSV, HEADER);
    
        """,
    dag=dag,
)

waterdt_ET >> postgres_prep >> waterdt_Load