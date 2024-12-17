#!/usr/bin/env python3
import requests
import argparse
import pandas as pandas
import zipfile
import os
from dotenv import load_dotenv
import form_url
from form_url import *
from zipfile import ZipFile 

load_dotenv

#extract water data class
class ExtractWaterETL:
    def __init__(self,url, state, year):
        self.url = url
        self.state = state
        self.year = year 
    
    def extract_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            resultfile_path = '/tmp/waterdataresult.csv'
            with open(resultfile_path, 'wb') as file:
                file.write(response.content)

        else:
            print('Unable to extract data!')

        return resultfile_path

#transform water data class
class TransformWaterETL:
    def __init__(self, resultfile_path):
        self.resultfile_path = resultfile_path

    def transform_prcs(self):

        wtrdf = pd.read_csv(self.resultfile_path)

        #result table
        result_tbl = wtrdf.filter(like='Result')[['ResultIdentifier','ResultDetectionConditionText','ResultSampleFractionText','ResultMeasureValue','ResultMeasure/MeasureUnitCode','ResultStatusIdentifier','ResultValueTypeName','ResultWeightBasisText','ResultTimeBasisText','ResultTemperatureBasisText','ResultParticleSizeBasisText','ResultCommentText','ResultDepthHeightMeasure/MeasureValue','ResultDepthHeightMeasure/MeasureUnitCode','ResultDepthAltitudeReferencePointText','ResultFileUrl','ResultAnalyticalMethod/MethodIdentifier','ResultAnalyticalMethod/MethodName','ResultAnalyticalMethod/MethodDescriptionText']]

        #activity table
        activity_tbl = wtrdf.filter(like='Activity')[['ActivityIdentifier','ActivityTypeCode','ActivityMediaName','ActivityMediaSubdivisionName','ActivityEndDate','ActivityStartDate','ActivityRelativeDepthName','ActivityDepthHeightMeasure/MeasureValue','ActivityDepthAltitudeReferencePointText','ActivityTopDepthHeightMeasure/MeasureValue','ActivityBottomDepthHeightMeasure/MeasureUnitCode','ActivityBottomDepthHeightMeasure/MeasureValue','ActivityCommentText']]
        #add foreign key
        activity_tbl['ResultIdentifier'] = result_tbl['ResultIdentifier']

        #location table
        location_tbl = wtrdf.filter(like='Location')[['ActivityLocation/LatitudeMeasure','ActivityLocation/LongitudeMeasure','MonitoringLocationName','MonitoringLocationIdentifier']]
        #add foreign key
        location_tbl['ResultIdentifier'] = result_tbl['ResultIdentifier']

        #detection table
        detection_tbl = wtrdf.filter(like='Detection')[['DetectionQuantitationLimitTypeName','DetectionQuantitationLimitMeasure/MeasureValue','DetectionQuantitationLimitMeasure/MeasureUnitCode']]
        #add foreign key
        detection_tbl['ResultIdentifier'] = result_tbl['ResultIdentifier']

        #sample table
        sample_tbl =  wtrdf.filter(like='Sample')[['SampleAquifer','SampleCollectionMethod/MethodIdentifier','SampleCollectionMethod/MethodIdentifier','SampleCollectionMethod/MethodDescriptionText','SampleCollectionEquipmentName','SampleTissueAnatomyName','LabSamplePreparationUrl']]
        #add foreign key
        sample_tbl['ResultIdentifier'] = result_tbl['ResultIdentifier']

        #organization table
        organization_tbl =  wtrdf.filter(like='Organization')[['OrganizationIdentifier','OrganizationFormalName']]
        #add foreign key
        organization_tbl['ResultIdentifier'] = result_tbl['ResultIdentifier']

        #characteristic table
        character_tbl = pd.concat([wtrdf.filter(like='Character'),wtrdf.filter(like='Hydrologic')])
        character_tbl['ResultIdentifier'] = result_tbl['ResultIdentifier']

        output_dir = '/app/output'

        waterData_tbls = [(result_tbl, f'{output_dir}/result_tbl.csv'),
                          (activity_tbl, f'{output_dir}/activity_tbl.csv'),
                          (location_tbl, f'{output_dir}/location_tbl.csv'),
                          (detection_tbl,f'{output_dir}/detection_tbl.csv'),
                          (sample_tbl, f'{output_dir}/sample_tbl.csv'),
                          (organization_tbl, f'{output_dir}/organization_tbl.csv'),
                          (character_tbl, f'{output_dir}/character_tbl.csv')]

        for tbl, tbl_file in waterData_tbls:
            tbl.to_csv(tbl_file, index=False)

        return waterData_tbls

    
    
    def parse_arguments():
        parser = argparse.ArgumentParser(description="Get state and year arguments passed in")

        parser.add_argument(
            '--state',
            type=str,
            required=True,
            help='Enter the state you would like to begin the process for'
        )

        parser.add_argument(
            '--year',
            type=int,
            required=True,
            help='Enter the year of data to be processed'
        )

        parser.add_argument(
            '--startyear',
            type=str,
            required=False,
            help='Enter the start year of data to be processed'
        )

        parser.add_argument(
            '--endyear',
            type=str,
            required=False,
            help='Enter the end year of data to be processed'
        )

        args = parser.parser_args()
        return args

    def main():
        args = parse_arguments()
        state = args.state
        year = args.year
        startyear = args.startyear 
        endyear = args.endyear 
        #create url class from form_url
        c_url = create_url(state, year, startyear, endyear)

        if state and year:
            url = c_url.getyear_url()
            extractionPrcs = ExtractWaterETL(url, state, year)
            resultfile_path = extractionPrcs.extract_data()

        elif state and startyear and endyear:
            url = c_url.rangeyear_url()
            extractionPrcs = ExtractWaterETL(url,state,startyear,endyear)
            resultfile_path = extractionPrcs.extract_data() 
        else:
            print(parser.format_help())

        if resultfile_path:
            if os.path.exists(resultfile_path):
                transform_data = TransformWaterETL(resultfile_path)
                transform_data.transform_prcs()
            else:
                print('Data transformation process failed')
        else:
            print('Data not found in file path')

if __name__ == "__main__":
    main()





