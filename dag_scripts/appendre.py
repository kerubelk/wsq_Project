from airflow import DAG
import calendar
import requests
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os, subprocess
import zipfile
from dateutil.relativedelta import relativedelta
from datetime import datetime,timedelta
default_args = {
        'startdate' : datetime(2024,10,24),
        }

#initializing dag
dag = DAG(dag_id='wsqetestone',
          default_args = default_args,
          start_date = days_ago(0),
          schedule_interval="0 0 1 */2 *",)


#first day and last day of the month calculation

now = datetime.now()
startdate = (now - relativedelta(months=1)).replace(day=1)
enddate = now.replace(day=1) - relativedelta(days=1)


startdate_str = startdate.strftime('%Y-%m-%d')
enddate_str = enddate.strftime('%Y-%m-%d')

#define extraction task function
def extract_data():
    response = requests.get(f'https://www.waterqualitydata.us/data/Result/search?countrycode=US&statecode=US%3A24&startDateLo={startdate_str}&startDateHi={enddate_str}&mimeType=csv&zip=yes&dataProfile=resultPhysChem&providers=NWIS&providers=STORET')
    with open('/tmp/resultwsq.zip', 'wb') as file:
        file.write(response.content)

#initialize extraction task
extractData = PythonOperator(
        task_id = 'extractData',
        python_callable = extract_data,
        dag=dag,
        )



def transform_data():
    #extract downloaded zip file
    with zipfile.ZipFile('/tmp/resultwsq.zip','r') as zip_ref:
        zip_ref.extractall('/tmp/')

    csv_path = '/tmp/resultphyschem.csv'
    if os.path.exists(csv_path):

        df=pd.read_csv(csv_path)

    else:
        raise FileNotFoundError(f"{csv_path} not found")

    #result table
    resdf=df.filter(like='Result')[['ResultIdentifier','ResultDetectionConditionText','ResultSampleFractionText','ResultMeasureValue','ResultMeasure/MeasureUnitCode','ResultStatusIdentifier','ResultValueTypeName','ResultWeightBasisText','ResultTimeBasisText','ResultTemperatureBasisText','ResultParticleSizeBasisText','ResultCommentText','ResultDepthHeightMeasure/MeasureValue','ResultDepthHeightMeasure/MeasureUnitCode','ResultDepthAltitudeReferencePointText','ResultFileUrl','ResultAnalyticalMethod/MethodIdentifier','ResultAnalyticalMethod/MethodIdentifier','ResultAnalyticalMethodName','ResultAnalyticalMethod/MethodDescriptionText']]

    resdf.to_csv('/var/lib/postgresql/resulttbl.csv',index=False)
    
    #activity table
    adf=df.filter(like='Activity')[['ActivitIdentifier','ActivityTypeCode','ActivityMediaName','ActivityMediaSubdivisionName','ActivityEndDate','ActivityStartDate','ActivityRelativeDepthName','ActivityDepthHeightMeasure/MeasureValue','ActivityDepthAltitudeReferencePointText','ActivityTopDepthHeightMeasureValue','ActivityBottomDepthHeightMeasureUnitCode','ActivityBottomDepthHeightMeasure/MeasureValue','ActivityCommentText']]
    adf['ResultIdentifier'] = resdf['ResultIdentifier']

    adf['ActivityStartDate'] = pd.to_datetime(adf['ActivityStartDate'])
    adf['ActivityEndDate'] = pd.to_datetime(adf['ActivityEndDate'])
    adf.to_csv('/var/lib/postgresql/activitytbl.csv',index=False)

    #location table
    ldf=df.filter(like='Location')[['ActivityLocation/LatitudeMeasure','ActivityLocation/LongitudeMeasure','MonitoringLocationName','MonitoringLocationIdentifier']]
    ldf['ResultIdentifier'] = resdf['ResultIdentifier']
    ldf.to_csv('/var/lib/postgresql/locationtbl.csv',index=False)

    #detection limi table
    detectdf=df.filter(like='Detect')[['DetectionQuantitationLimitTypeName','DetectionQuantitationLimitMeasure/MeasureValue','DetectionQuantitationLimitMeasure/MeasureUnitCode']]
    detectdf['ResultIdentifier'] = resdf['ResultIdentifier']
    
    detectdf.to_csv('/var/lib/postgresql/detectionlimittbl.csv',index=False)

    #sample table
    sampdf=df.filter(like='Sample')[['SampleAquifer','SampleCollectionMethod/MethodIdentifier','SampleCollectionMethod/MethodIdentifier','SampleCollectionMethod/MethodDescriptionText','SampleCollectionEquipmentName','SampleTissueAnatomyName','LabSamplePreparationUrl']]
    sampdf['ResultIdentifier'] = resdf['ResultIdentifier']
    sampdf.to_csv('/var/lib/postgresql/sampletbl.csv', index=False)

    #organization table
    orgdf=df.filter(like='Organization')[['OrganizationIdentifier','OrganizationFormalName']]
    orgdf['ResultIdentifier'] = resdf['ResultIdentifier']
    orgdf.to_csv('/var/lib/postgresql/organizationtbl.csv',index=False)

    #characteristic table
    characterdf=pd.concat([df.filter(like='Character'),df.filter(like='Hydrologic')])
    characterdf['ResultIdentifier'] = resdf['ResultIdentifier']
    characterdf.to_csv('/var/lib/postgresql/characteristictbl.csv',index=False)

#initialize transformation task
transformData = PythonOperator(
        task_id='transformData',
        python_callable=transform_data,
        dag=dag,
        )
    

#increment data to data base task
increment_tables = PostgresOperator(
            task_id='load_tables',
            postgres_conn_id='wsqconn',
            sql = """

            COPY result (resultidentifier, resultdetectionconditiontext, resultsamplefractiontext, resultmeasurevalue, resultmeasureunitcode, resultstatusidentifier, resultvaluetypename, resultweightbasistext, resulttimebasistext, resulttemperaturebasistext, resultparticlesizebasistext, resultcommenttext, resultdepthheightmeasurevalue, resultdepthheightmeasureunitcode, resultdepthaltitudereferencepointtext, resultfileurl, resultanalyticalmethodidentifier, resultanalyticalmethodname, resultanalyticalmethoddescriptiontext,resultdetectionlimiturl,resultlaboratorycommenttext) FROM '/var/lib/postgresql/resulttbl.csv' WITH (FORMAT CSV, HEADER);



            COPY activity (activityidentifier, activitytypecode, activitymedianame, activitymediasubdivisionname, activitystartdate, activityenddate, activityrelativedepthname, activitydepthheightmeasurevalue, activitydepthaltitudereferencepointtext, activitytopdepthheightmeasurevalue, activitytopdepthheightmeasureunitcode, activitybottomdepthheightmeasurevalue, activitycommenttext,resultid) FROM '/var/lib/postgresql/activitytbl.csv' WITH (FORMAT CSV, HEADER);



            COPY charactersitic (characteristicname, hydrologicevent, hydrologiccondition,resultid) FROM '/var/lib/postgresql/characteristictbl.csv' WITH (FORMAT CSV, HEADER);


            COPY detectionlimit (detectionlimittypename, detectionlimitmeasurevalue, detectionlimitmeasureunitcode,resultid) FROM '/var/lib/postgresql/detectionlimittbl.csv' WITH (FORMAT CSV, HEADER);            

            COPY location (monitoringlocationname, monitoringlocationid, latitude, longitude,resultid) FROM '/var/lib/postgresql/locationtbl.csv' WITH (FORMAT CSV, HEADER);


            COPY organization (organizationformalname, organizationalidentifier,resultid) FROM '/var/lib/postgresql/organizationtbl.csv' WITH (FORMAT CSV, HEADER);

            COPY sample (samplecollectionmethodid, samplecollectionmethodidcontext, samplecollectionmethodname, samplecollectionmethoddescriptiontext, samplecollectionequipmentname, sampletissueanatomyname, sampleaquifer, lamsampleurl,resultid) FROM '/var/lib/postgresql/sampletbl.csv' WITH (FORMAT CSV, HEADER);

            """,
            dag=dag



        )

#clean up task
cleanup = BashOperator(
        task_id='cleanup',
        bash_command=""" rm /tmp/*.csv || true
                         rm /var/lib/postgresql/*.csv || true
        """,

        dag=dag
        )


#order of execution for tasks
extractData >> transformData >> increment_tables >> cleanup







