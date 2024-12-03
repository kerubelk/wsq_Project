import datetime 
import zipfile
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import numpy as np

default_args = {
        'startdate': datetime.datetime(2024,11,25),

        }
dag=DAG(dag_id='wsqhistory',
        default_args=default_args,
        start_date= days_ago(0),

        schedule_interval=None,)


def extract_history():
    
    for year in range (2008,2015):


    
        res = requests.get(f'https://www.waterqualitydata.us/data/Result/search?countrycode=US&statecode=US%3A24&startDateLo=01-01-{year}&startDateHi=12-31-{year}&mimeType=csv&zip=yes&dataProfile=resultPhysChem&providers=NWIS&providers=STORET')



        with open(f'/tmp/result{year}.zip','wb') as file:
            file.write(res.content)


extract_history = PythonOperator(
        task_id='extractHistory',
        python_callable=extract_history,
        dag=dag,
        )



def transform_history():
    

    chunk_size = 10000
    df_list = []
    for chunk in pd.read_csv('/home/kb/wsqhistory_hold/resultphyschem.csv',chunksize=chunk_size):

    
        df_list.append(chunk)

    df = pd.concat(df_list, ignore_index=True)


    resultdf=df.filter(like='Result')[['ResultIdentifier','ResultDetectionConditionText','ResultSampleFractionText','ResultMeasureValue','ResultMeasure/MeasureUnitCode','ResultStatusIdentifier','ResultValueTypeName','ResultWeightBasisText','ResultTimeBasisText','ResultTemperatureBasisText','ResultParticleSizeBasisText','ResultCommentText','ResultDepthHeightMeasure/MeasureValue','ResultDepthHeightMeasure/MeasureUnitCode','ResultDepthAltitudeReferencePointText','ResultFileUrl','ResultAnalyticalMethod/MethodIdentifier','ResultAnalyticalMethod/MethodName','ResultAnalyticalMethod/MethodDescriptionText','ResultDetectionQuantitationLimitUrl','ResultLaboratoryCommentText']]

    resultdf.to_csv('/var/lib/postgresql/Result_Hist.csv',index=False)

    adf=df.filter(like='Activity')[['ActivityIdentifier','ActivityTypeCode','ActivityMediaName','ActivityMediaSubdivisionName','ActivityEndDate','ActivityStartDate','ActivityRelativeDepthName','ActivityDepthHeightMeasure/MeasureValue','ActivityDepthAltitudeReferencePointText','ActivityTopDepthHeightMeasure/MeasureValue','ActivityBottomDepthHeightMeasure/MeasureUnitCode','ActivityBottomDepthHeightMeasure/MeasureValue','ActivityCommentText']]



    adf['ResultIdentifier']=resultdf['ResultIdentifier']
    adf['ActivityStartDate'] = pd.to_datetime(adf['ActivityStartDate'])
    adf['ActivityEndDate'] = pd.to_datetime(adf['ActivityEndDate'])

    adf.to_csv('/var/lib/postgresql/Activity_Hist.csv',index=False)

    ldf=df.filter(like='Location')[['ActivityLocation/LatitudeMeasure','ActivityLocation/LongitudeMeasure','MonitoringLocationName','MonitoringLocationIdentifier']]

    ldf['ResultIdentifier']=resultdf['ResultIdentifier']
    ldf.to_csv('/var/lib/postgresql/Location_hist{file_counter}.csv',index=False)

    detectdf=df.filter(like='Detect')[['DetectionQuantitationLimitTypeName','DetectionQuantitationLimitMeasure/MeasureValue','DetectionQuantitationLimitMeasure/MeasureUnitCode']]

    detectdf['ResultIdentifier']=resultdf['ResultIdentifier']

    detectdf.to_csv('/var/lib/postgresql/Detection_Hist.csv',index=False)

    sampledf=df.filter(like='Sample')[['SampleAquifer','SampleCollectionMethod/MethodIdentifier','SampleCollectionMethod/MethodIdentifier','SampleCollectionMethod/MethodDescriptionText','SampleCollectionEquipmentName','SampleTissueAnatomyName','LabSamplePreparationUrl']]
    sampledf['ResultIdentifier']=resultdf['ResultIdentifier']
    sampledf.to_csv('/var/lib/postgresql/Sample_Hist.csv',index=False)


    orgdf=df.filter(like='Organization')[['OrganizationIdentifier','OrganizationFormalName']]
    orgdf['ResultIdentifier']=resultdf['ResultIdentifier']
    orgdf.to_csv('/var/lib/postgresql/Organization_Hist.csv',index=False)

    characterdf=pd.concat([df.filter(like='Character'),df.filter(like='Hydrologic')])
    characterdf['ResultIdentifier']=resultdf['ResultIdentifier']


    characterdf.to_csv('/var/lib/postgresql/Characteristic_Hist.csv',index=False)

                

transform_hist = PythonOperator(
        task_id='transformHist',
        python_callable=transform_history,
        dag=dag,
        )



def insertElasticsearch():

    df1=pd.read_csv('/var/lib/postgresql/Result_Hist.csv',low_memory=False)
    df2=pd.read_csv('/var/lib/postgresql/Activity_Hist.csv',low_memory=False)
    df3=pd.read_csv('/var/lib/postgresql/Detection_Hist.csv',low_memory=False)
    df4=pd.read_csv('/var/lib/postgresql/Location_Hist.csv',low_memory=False)
    df5=pd.read_csv('/var/lib/postgresql/cleanedSample_Hist.csv',low_memory=False)
    df6=pd.read_csv('/var/lib/postgresql/Organization_Hist.csv',low_memory=False)
    df7=pd.read_csv('/var/lib/postgresql/Characteristic_Hist.csv',low_memory=False)


    dfs = [df1,df2,df3,df4,df5]
    for df in dfs:
        df=df.replace({np.nan: None})
        es = Elasticsearch()
        actions = [
                {
                "_index": "wsqhistory_old",
                "_source": r.to_dict(),
                


                }
                for _, r in df.iterrows()
            

                   ]
        helpers.bulk(es,actions, chunk_size=1000)

    
    


insertElasticsearch = PythonOperator(
        task_id='insertElasticsearch',
        python_callable = insertElasticsearch,
        dag=dag,
        )



cleanup = BashOperator(
        task_id='cleanUp',
        bash_command = """  
                rm -f /home/kb/wsqhistory_old/* || true
                rm -f /tmp/* || true
        
                        """,
        dag=dag,
        )
extract_history >> transform_hist >> insertElasticsearch >> cleanup
