from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.utils.dates import days_ago
import os,subprocess
import requests
import pandas as pd
from datetime import datetime, timedelta
import calendar
from airflow.operators.bash import BashOperator



dag = DAG(dag_id='waterdataETL',
	  start_date = days_ago(0),
	  schedule_interval="0 0 L * *",) # Last day of every month at midnight


def extractData():
    startdate = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1)
    enddate = startdate.replace(day=calendar.monthrange(startdate.year,startdate.month)[1])

    startdate_str = startdate.strftime('%Y-%m-%d')
    enddate_str = enddate.strftime('%Y-%m-%d')
    fullurl=f"https://www.waterqualitydata.us/wqx3/Result/search?countrycode=US&statecode=US%3A24&countycode=US%3A24%3A031&startDateLo={startdate_str}&startDateHi={enddate_str}&mimeType=csv&dataProfile=fullPhysChem&providers=NWIS&providers=STORET"


    stationurl = f"https://www.waterqualitydata.us/wqx3/Station/search?countrycode=US&statecode=US%3A24&countycode=US%3A24%3A031&startDateLo={startdate_str}&startDateHi={enddate_str}&mimeType=csv&providers=NWIS&providers=STORET"

    narrowurl=f"https://www.waterqualitydata.us/wqx3/Result/search?countrycode=US&statecode=US%3A24&countycode=US%3A24%3A031&startDateLo={startdate_str}&startDateHi={enddate_str}&mimeType=csv&dataProfile=narrow&providers=NWIS&providers=STORET"

    urls = [
        (fullurl,'/tmp/Result.csv'),
        (stationurl,'/tmp/Station.csv'),
        (narrowurl,'/tmp/Narrow.csv')
        ]

    for url,csv in urls:
        response=requests.get(url,stream=True)
        with open(csv, 'wb') as file:
    	    file.write(response.content) 


        if 'Station' in csv:
            sdf=pd.read_csv(csv)
	   

        elif 'Result' in csv:
            fdf=pd.read_csv(csv)
	    

        elif 'Narrow' in csv:
            ndf=pd.read_csv(csv)
	    


    
extractdata = PythonOperator(
	task_id='extractData',
	python_callable=extractData,
	dag=dag,

)
def transformData():
    sdf=pd.read_csv('/tmp/Station.csv')
    fdf=pd.read_csv('/tmp/Result.csv')
    ndf=pd.read_csv('/tmp/Narrow.csv')

    #normalize
    resultdf=fdf.filter(like='Result')[['Result_MeasureIdentifier','Result_ResultDetectionCondition','Result_SampleFraction','Result_Measure','Result_MeasureUnit','Result_MeasureStatusIdentifier','Result_MeasureType','Result_WeightBasis','Result_TimeBasis','Result_MeasureTemperatureBasis','Result_MeasureParticleSizeBasis','DataQuality_ResultComment','ResultDepthHeight_Measure','ResultDepthHeight_MeasureUnit','ResultDepthHeight_AltitudeReferencePoint','ResultAttachment_FileDownload','ResultAnalyticalMethod_Identifier','ResultAnalyticalMethod_IdentifierContext','ResultAnalyticalMethod_Name','ResultAnalyticalMethod_Description']]

    activitydf=fdf.filter(like='Activity')[['Activity_ActivityIdentifier','Activity_TypeCode','Activity_Media','Activity_MediaSubdivisionName','Activity_EndDate','Activity_StartDate','Activity_ActivityRelativeDepth','Activity_DepthHeightMeasure','Activity_BottomDepthAltitudeReferencePoint','Activity_TopDepthMeasure','Activity_TopDepthMeasureUnit','Activity_BottomDepthMeasure','Activity_Comment']]

    activitydf['Result_MeasureIdentifier'] = resultdf['Result_MeasureIdentifier']

    sampletbldf=fdf.filter(like='Sample')[['SampleCollectionMethod_Identifier','SampleCollectionMethod_IdentifierContext','SampleCollectionMethod_Name','SampleCollectionMethod_Description','SampleCollectionMethod_EquipmentName','ResultBiological_SampleTissueAnatomy']]

    sampletbldf['Result_MeasureIdentifier'] = resultdf['Result_MeasureIdentifier']

    locationdf=sdf.filter(like='Location')[['Location_Latitude','Location_Longitude','Location_Name','Location_Identifier']]

    locationdf['Result_MeasureIdentifier'] = resultdf['Result_MeasureIdentifier']
    orgdf=sdf.filter(like='Org_')
    orgdf['Result_MeasureIdentifier'] = resultdf['Result_MeasureIdentifier']

    characterdf=pd.concat([fdf.filter(like='Character'),fdf.filter(like='Hydrologic')])[['Result_Characteristic' ,'Activity_HydrologicCondition','Activity_HydrologicEvent']]


    characterdf['Result_MeasureIdentifier'] = resultdf['Result_MeasureIdentifier']

    detectdf=ndf.filter(like='Detection')[['DetectionLimit_TypeA','DetectionLimit_MeasureA','DetectionLimit_MeasureUnitA']]

    detectdf['Result_MeasureIdentifier'] = resultdf['Result_MeasureIdentifier']
    loaddirectory = [
            (resultdf, '/var/lib/postgresql/Result.csv'),
            (activitydf, '/var/lib/postgresql/Activity.csv'),
            (sampletbldf,'/var/lib/postgresql/Sample.csv'),
            (locationdf,'/var/lib/postgresql/Location.csv'),
            (orgdf,'/var/lib/postgresql/Organization.csv'),
            (characterdf,'/var/lib/postgresql/Characteristic.csv'),
            (detectdf,'/var/lib/postgresql/Detectionlimit.csv')
            ]

    for df, path in loaddirectory:
    	if 'Unnamed: 0' in df:
    	    del df['Unnamed: 0']
    	df.to_csv(path,index=False)
transformdata = PythonOperator(
	task_id='transformData',
	python_callable=transformData,
	dag=dag,
)	




loadData=PostgresOperator(
	task_id='loadData',
	postgres_conn_id='wsqconn',
	sql = """
	COPY result (resultidentifier, resultdetectionconditiontext, resultsamplefractiontext, resultmeasurevalue,resultmeasureunitcode,resultstatusidentifier,resultvaluetypename,resultweightbasistext,resulttimebasistext,resulttemperaturebasistext,resultparticlesizebasistext,resultcommenttext,resultdepthheightmeasurevalue,resultdepthheightmeasureunitcode,resultdepthaltitudereferencepointtext, resultfileurl,resultanalyticalmethodidentifier,resultanalyticalmethodidentifiercontext,resultanalyticalmethodname, resultanalyticalmethoddescriptiontext) FROM '/var/lib/postgresql/Result.csv' WITH (FORMAT CSV, HEADER);

	COPY activity (activityidentifier, activitytypecode, activitymedianame, activitymediasubdivisionname, activityenddate, activitystartdate, activityrelativedepthname, activitydepthheightmeasurevalue,activitydepthaltitudereferencepointtext,activitytopdepthheightmeasurevalue,activitytopdepthheightmeasureunitcode,activitybottomdepthheightmeasurevalue,activitycommenttext,resultid) FROM '/var/lib/postgresql/Activity.csv' WITH (FORMAT CSV, HEADER);
	
	COPY charactersitic (characteristicname,hydrologicevent, hydrologiccondition, resultid) FROM '/var/lib/postgresql/Characteristic.csv' WITH (FORMAT CSV, HEADER);

 
	COPY detectionlimit (detectionlimittypename, detectionlimitmeasurevalue, detectionlimitmeasureunitcode, resultid) FROM '/var/lib/postgresql/Detectionlimit.csv' WITH (FORMAT CSV, HEADER);

	COPY location (latitude, longitude,monitoringlocationname,monitoringlocationid, resultid) FROM '/var/lib/postgresql/Location.csv' WITH (FORMAT CSV, HEADER);

	COPY organization (organizationidentifier,organizationformalname, resultid) FROM '/var/lib/postgresql/Organization.csv' WITH (FORMAT CSV, HEADER);

	COPY sample (samplecollectionmethodid, samplecollectionmethodidcontext, samplecollectionmethodname, samplecollectionmethoddescriptiontext, samplecollectionequipmentname, sampletissueanatomyname, resultid) FROM '/var/lib/postgresql/Sample.csv' WITH (FORMAT CSV, HEADER);

 	

		""",
		dag=dag
	

)

cleanup = BashOperator(
        task_id = "cleanup",
        bash_command = """   

            rm -f /home/kb/wsqhistory_old/* || true
            rm  -f /tmp/* || true

        """,
        dag=dag,
        )





extractdata >> transformdata >> loadData >> cleanup


