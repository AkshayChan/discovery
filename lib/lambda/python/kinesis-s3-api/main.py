"""This function is used to update the Kinesis streams with the race data when the API is triggered"""

import json
import boto3
from botocore.exceptions import ClientError
import logging
import datetime
import os
import copy
from collections.abc import MutableMapping
import random as rand

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""Initialise required clients/environment variables"""

KINESIS_SINK = os.environ['KINESIS_SINK']  # kinesis stream where to put livestream data
DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE']  # dynamodb table to update the pre season questionnaire
OUTPUT_S3_BUCKET = os.environ['OUTPUT_S3_BUCKET']
OUTPUT_S3_KEY = os.environ['OUTPUT_S3_KEY']

kinesis_client = boto3.client('kinesis', region_name='eu-west-1')
dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
partition_day = 'date_part=' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d-%H:%M:%S')

live_data_list = ['/StoreLiveRidersTracking', '/StoreLiveRidersData', '/StoreStartTime', '/StoreLapCounter',
                  '/StoreRiderEliminated', '/StoreFinishTime', '/StoreRaceStartLive']


# @logger.inject_lambda_context(log_event=True)
def handler(event, context):
    '''
       Lambda handler function, responsible for managing the incoming requests though the events
    '''
    # Parse the JSON string payload
    body = json.loads(event['body'])
    logger.info("Triggered API endpoint: %s", event['path'])
    logger.info("Body: %s", body)
    whole_event_path = 'testEvent/{}/{}-{}.json'.format(partition_day,
                                                        datetime.datetime.strftime(datetime.datetime.now(),
                                                                                   '%Y-%m-%d-%H:%M:%S.%f'),
                                                        rand.random())

    save_json_to_s3(OUTPUT_S3_BUCKET, whole_event_path, event)  # STORE EVENT

    # validate the data input
    if type(body) == list:
        for item in body:
            data_val_output = validate_data_input(event['path'], item)
            if data_val_output:
                break
    else:
        data_val_output = validate_data_input(event['path'], body)
    if data_val_output:
        logger.error(data_val_output)
        return response_body(500, event['path'], data_val_output)
    else:
        logger.info("Data input validation successful")

    if event['path'] in live_data_list:
        logger.info("Pushing the real time events to the kinesis stream %s", KINESIS_SINK)
        body["ApiIngestTime"] = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S.%f')[:-3]
        return push_to_kinesis(body, event['path'], KINESIS_SINK)

    elif event['path'] == '/StorePreSeasonQuestionnaire':
        try:
            logger.info("Pushing the questionnaire list data to the S3 bucket %s", OUTPUT_S3_BUCKET)
            questionnaire_list_s3_key = OUTPUT_S3_KEY + 'PreSeasonQuestionnaire/{}/{}.json'.format(partition_day,
                                                                                                   timestamp)
            questionnaire_list_current_version_s3_key = OUTPUT_S3_KEY + 'PreSeasonQuestionnaire/current_version.json'
            save_json_to_s3(OUTPUT_S3_BUCKET, questionnaire_list_s3_key, body)
            save_json_to_s3(OUTPUT_S3_BUCKET, questionnaire_list_current_version_s3_key, body)
            for item in body:
                try:
                    logger.info(
                        "Pushing the questionnaire data for UCIID %s to the DynamoDB table", item["UCIID"])
                    ddb_item = convert_record_to_ddb_item(item, "questionnaire")
                    save_item_to_ddb(DYNAMODB_TABLE, ddb_item)
                except Exception as e:
                    logger.error(e)
                    return response_body(500, event['path'], "Data not saved - please validate schema and values")
            return response_body(200, event['path'], "questionnaire list")
        except Exception as e:
            logger.error(e)
            return response_body(400, event['path'], "questionnaire list payload")

    elif event['path'] == '/StoreRacesList':
        try:
            event_id = body['EventID']
            logger.info("Pushing the race list data to the S3 bucket %s", OUTPUT_S3_BUCKET)
            race_list_s3_key = OUTPUT_S3_KEY + 'RacesList/{}/{}.json'.format(partition_day, timestamp)
            save_json_to_s3(OUTPUT_S3_BUCKET, race_list_s3_key, body)
            race_data = copy.deepcopy(body)
            race_data.pop("Races")

            for item in body["Races"]:
                try:
                    logger.info("Pushing the race data for race id %s to the DynamoDB table", item['RaceID'])
                    race_item = {**race_data, **item}
                    ddb_item = convert_record_to_ddb_item(race_item, "race_list")
                    save_item_to_ddb(DYNAMODB_TABLE, ddb_item)
                except Exception as e:
                    logger.error(e)
                    return response_body(500, event['path'], "Data not saved - please validate schema and values")
                # we have to generate artificial object based on this data with Round level statistics
            try:
                generate_and_save_round_level_object(body["Races"])
            except Exception as e:
                logger.error(e)
                return response_body(500, event['path'], "Data for EventID=%s not saved - please validate schema and values" % event_id)
            return response_body(200, event['path'], "race list")
        except Exception as e:
            logger.error(e)
            return response_body(400, event['path'], "race list payload")

    elif event['path'] == '/StoreRaceStartList':
        try:
            race_id = body['RaceID']
            race_start_list_s3_key = OUTPUT_S3_KEY + "RaceStartList/{}/race_part={}/{}.json".format(partition_day,
                                                                                                    race_id,
                                                                                                    timestamp)
            logger.info("Pushing the race start list data for race id: %s to the S3 bucket %s", race_id,
                        OUTPUT_S3_BUCKET)
            save_json_to_s3(OUTPUT_S3_BUCKET, race_start_list_s3_key, body)
            rider_data = copy.deepcopy(body)
            rider_data.pop("Startlist")
            body = append_predicted_rank(body)

            for item in body["Startlist"]:
                try:
                    logger.info(
                        "Pushing the race start data for rider id %s to the DynamoDB table", item['UCIID'])
                    rider_item = {**rider_data, **item}
                    ddb_item = convert_record_to_ddb_item(rider_item, "race_start_list")
                    save_item_to_ddb(DYNAMODB_TABLE, ddb_item)
                except Exception as e:
                    logger.error(e)
                    return response_body(500, event['path'], "Data not saved - please validate schema and values")
            # we have to update artificial object based on this data with Round level statistics
            try:
                update_round_level_object_with_startlist(body)
            except Exception as e:
                logger.error(e)
                return response_body(500, event['path'], "Data for RaceID=%s not saved - please check if this race was in RaceList or Race is has correct Rounds/heats and raceType=Sprint|Elimination|Scratch|Kerin" % race_id)
            return response_body(200, event['path'], "race id: {} start list".format(race_id))
        except Exception as e:
            logger.error(e)
            return response_body(400, event['path'], "race id: {} start list payload".format(race_id))

    elif event['path'] == '/StoreRaceResults':
        try:
            race_id = body['RaceID']
            race_results_s3_key = OUTPUT_S3_KEY + "RaceResults/{}/race_part={}/{}.json".format(partition_day,
                                                                                               race_id, timestamp)
            logger.info("Pushing the race results data for race id: %s to the S3 bucket %s", race_id,
                        OUTPUT_S3_BUCKET)
            save_json_to_s3(OUTPUT_S3_BUCKET, race_results_s3_key, body)
            rider_data = copy.deepcopy(body)
            rider_data.pop("Results")
            rider_data["RaceLaps"] = rider_data.pop("Laps")
            for item in body["Results"]:
                try:
                    logger.info(
                        "Pushing the race results data for rider id %s to the DynamoDB table", item['UCIID'])
                    rider_item = {**rider_data, **item}
                    ddb_item = convert_record_to_ddb_item(rider_item, "race_results")
                    save_item_to_ddb(DYNAMODB_TABLE, ddb_item)
                except Exception as e:
                    logger.error(e)
                    return response_body(500, event['path'], "Data not saved - please validate schema and values")
            return response_body(200, event['path'], "race id: {} results".format(race_id))
        except Exception as e:
            logger.error(e)
            return response_body(400, event['path'], "race id: {} results payload".format(race_id))

    elif event['path'] == '/StoreClassification':
        logger.info("etered into classification")
        try:
            event_id = body['EventID']
            race_results_s3_key = OUTPUT_S3_KEY + "Classification/{}/race_part={}/{}.json".format(partition_day,
                                                                                                  event_id, timestamp)
            logger.info("Pushing the race results data for race id: %s to the S3 bucket %s", event_id,
                        OUTPUT_S3_BUCKET)
            save_json_to_s3(OUTPUT_S3_BUCKET, race_results_s3_key, body)
            rider_data = copy.deepcopy(body)
            rider_data.pop("Results")
            for item in body["Results"]:
                try:
                    logger.info(
                        "Pushing the classification data for rider id %s to the DynamoDB table", item['UCIID'])
                    rider_item = {**rider_data, **item}
                    ddb_item = convert_record_to_ddb_item(rider_item, "classification")
                    save_item_to_ddb(DYNAMODB_TABLE, ddb_item)
                except Exception as e:
                    logger.error(e)
                    return response_body(500, event['path'], "Data not saved - please validate schema and values")
            return response_body(200, event['path'], "event id: {} classification".format(event_id))
        except Exception as e:
            logger.error(e)
            return response_body(400, event['path'], "event id: {} classification payload".format(event_id))

    else:
        logger.error("Check the RaceData endpoint you are hitting")
        return response_body(400, event['path'], "URL/endpoint")


def response_body(code, endpoint, data):
    '''
    Response bodies based on the scenario
    :param int code: response code
    :param str endpoint: endpoint name
    :param str data: Live or race data
    :return: dict
    '''

    # Update the kinesis stream/save data to the S3 bucket
    if code == 200:
        response = {"endpoint": endpoint, "msg": "Successfully saved the data: {}".format(data)}
    elif code == 400:
        response = {"endpoint": endpoint, "msg": "ERROR - Check: {}".format(data)}
    elif code == 500:
        response = {"endpoint": endpoint, "msg": "ERROR - {}".format(data)}

    logger.info("Returning code: %s and message: %s", code, response)

    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,x-api-key,access-control-allow-origin,*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(response)
    }


def push_to_kinesis(record, endpoint, stream_name):
    '''
    Saves data to kinesis
    :param str record: data to be saved as kinesis record
    :param str endpoint: endpoint which we triggered
    :param str stream_name: kinesis stream name
    :return: None
    '''
    try:
        kinesis_response = kinesis_client.put_record(
            Data=json.dumps(record) + "\n",
            PartitionKey=str(hash(time_now_str())),
            StreamName=stream_name
        )
        logger.debug("Kinesis response: {}".format(kinesis_response))
        return response_body(200, endpoint, "live")
    except Exception as e:
        logger.error(e)
        return response_body(500, endpoint, "Data NOT saved - please validate schema and values")


def time_now_str():
    '''
    Generates start date like 2021-07-06 05:59:16.462
    :return: actual timestamp
    :rtype: str
    '''
    return datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S.%f')[:-3]


def save_json_to_s3(dest_bucket_name, dest_object_name, data):
    '''
    Saves data to s3
    :param str dest_bucket_name: bucket name
    :param str dest_object_name: s3 key
    :param str data: data to be saved in this location
    :return: None
    '''
    try:
        s3_client.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=json.dumps(data))
    except ClientError as e:
        logger.error(e)
        return False


def save_item_to_ddb(table_name, ddb_item):
    '''
    Saves data to dynamodb
    :param str table_name: table name
    :param str ddb_item: data item to be saved to the table
    :return: None
    '''
    dynamodb_client.put_item(TableName=table_name, Item=ddb_item)


def convert_record_to_ddb_item(data_item, data_type):
    '''
    Converts data to the correct format to push to DynamoDB
    :param str data_item: record in regular JSON
    :param str data_type: static or types of race data
    :return: ddb_item
    '''
    ddb_item = {}

    if data_type == "questionnaire":
        ddb_item = {
            'pk': {'S': 'RIDER#UCIID=' + str(data_item['UCIID']) + '#'},
            'sk': {'S': 'RIDER#'},
            'FirstName': {'S': str(data_item['FirstName'])},
            'LastName': {'S': str(data_item['LastName'])},
            'BirthDate': {'S': str(data_item['BirthDate'])},
            'UCIID': {'S': str(data_item['UCIID'])},
            'Bip': {'S': str(data_item['Bip'])},
            'Gender': {'S': str(data_item['Gender'])},
            'TrainingLocation': {'S': str(data_item['TrainingLocation'])},
            'LeagueCat': {'S': str(data_item['LeagueCat'])},
            'Nationality': {'S': str(data_item['Nationality'])},
            'SeasonTitle': {'S': str(data_item['SeasonTitle'])},
            'HeightCm': {'S': str(data_item['HeightCm'])},
            'WeightKg': {'S': str(data_item['WeightKg'])},
            'RestHrBpm': {'S': str(data_item['RestHrBpm'])},
            'MaxHrBpm': {'S': str(data_item['MaxHrBpm'])},
            'Flying200': {'S': str(data_item['Flying200'])},
            'GearingForFlying200': {'S': str(data_item['GearingForFlying200'])},
            'PowerPeakW': {'S': str(data_item['PowerPeakW'])},
            'Power5sW': {'S': str(data_item['Power5sW'])},
            'Power15sW': {'S': str(data_item['Power15sW'])},
            'Power30sW': {'S': str(data_item['Power30sW'])},
            'Power60sW': {'S': str(data_item['Power60sW'])},
            'Power120sW': {'S': str(data_item['Power120sW'])},
            'Power180sW': {'S': str(data_item['Power180sW'])},
            'Power300sW': {'S': str(data_item['Power300sW'])},
            'Power600sW': {'S': str(data_item['Power600sW'])},
            'Power1200sW': {'S': str(data_item['Power1200sW'])},
            'Power1800sW': {'S': str(data_item['Power1800sW'])},
            'Power3600sW': {'S': str(data_item['Power3600sW'])},
        }

    if data_type == "race_list":
        ddb_item = {
            'pk': {'S': 'EVENT#EventID=' + str(data_item['EventID']) + '#'},
            'sk': {'S': 'RACE#RaceID=' + str(data_item['RaceID']) + '#'},
            'Message': {'S': str(data_item['Message'])},
            'SeasonID': {'N': str(data_item['SeasonID'])},
            'EventID': {'N': str(data_item['EventID'])},
            'TimeStamp': {'S': str(data_item['TimeStamp'])},
            'Date': {'S': str(data_item['Date'])},
            'EventName': {'S': str(data_item['EventName'])},
            'RaceID': {'N': str(data_item['RaceID'])},
            'RaceType': {'S': str(data_item['RaceType'])},
            'Gender': {'S': str(data_item['Gender'])},
            'League': {'S': str(data_item['League'])},
            'Heat': {'N': str(data_item['Heat'])},
            'TotalHeats': {'N': str(data_item['TotalHeats'])},
            'Round': {'N': str(data_item['Round'])},
            'TotalRounds': {'N': str(data_item['TotalRounds'])},
            'Laps': {'N': str(data_item['Laps'])},
            'Distance': {'N': str(data_item['Distance'])},
            'RaceName': {'S': str(data_item['RaceName'])},
            'StartTime': {'S': str(data_item['StartTime'])},
            'RaceStatus': {'S': 'SCHEDULED'},
        }

    if data_type == "race_start_list":
        ddb_item = {
            'pk': {'S': 'RIDER#UCIID=' + str(data_item['UCIID']) + '#'},
            'sk': {'S': 'RACE#RaceID=' + str(data_item['RaceID']) + '#'},
            'Message': {'S': str(data_item['Message'])},
            'SeasonID': {'N': str(data_item['SeasonID'])},
            'EventID': {'N': str(data_item['EventID'])},
            'RaceID': {'N': str(data_item['RaceID'])},
            'Gender': {'S': str(data_item['Gender'])},
            'RaceType': {'S': str(data_item['RaceType'])},
            'League': {'S': str(data_item['League'])},
            'Heat': {'N': str(data_item['Heat'])},
            'TotalHeats': {'N': str(data_item['TotalHeats'])},
            'Round': {'N': str(data_item['Round'])},
            'TotalRounds': {'N': str(data_item['TotalRounds'])},
            'RaceName': {'S': str(data_item['RaceName'])},
            'RaceLaps': {'N': str(data_item['Laps'])},
            'Distance': {'N': str(data_item['Distance'])},
            'TimeStamp': {'S': str(data_item['TimeStamp'])},
            'Bib': {'S': str(data_item['Bib'])},
            'UCIID': {'N': str(data_item['UCIID'])},
            'FirstName': {'S': str(data_item['FirstName'])},
            'LastName': {'S': str(data_item['LastName'])},
            'ShortTVName': {'S': str(data_item['ShortTVName'])},
            'Team': {'S': str(data_item['Team'])},
            'NOC': {'S': str(data_item['NOC'])},
            'Status': {'S': str(data_item['Status'])},
            'StartPosition': {'N': str(data_item['StartPosition'])},
            'StartingLane': {'N': str(data_item['StartingLane'])},
            'PredictedRank': {'N': str(data_item.get('PredictedRank', ""))}
        }

    if data_type == "race_results":
        ddb_item = {
            'pk': {'S': 'RIDER#UCIID=' + str(data_item['UCIID']) + '#'},
            'sk': {'S': 'RESULTS#RaceID=' + str(data_item['RaceID']) + '#'},
            'Message': {'S': str(data_item['Message'])},
            'SeasonID': {'N': str(data_item['SeasonID'])},
            'EventID': {'N': str(data_item['EventID'])},
            'RaceID': {'N': str(data_item['RaceID'])},
            'RaceType': {'S': str(data_item['RaceType'])},
            'Gender': {'S': str(data_item['Gender'])},
            'League': {'S': str(data_item['League'])},
            'Heat': {'N': str(data_item['Heat'])},
            'TotalHeats': {'N': str(data_item['TotalHeats'])},
            'Round': {'N': str(data_item['Round'])},
            'TotalRounds': {'N': str(data_item['TotalRounds'])},
            'State': {'S': str(data_item['State'])},
            'RaceName': {'S': str(data_item['RaceName'])},
            'RaceLaps': {'N': str(data_item['RaceLaps'])},
            'Distance': {'N': str(data_item['Distance'])},
            'RaceTime': {'S': str(data_item['RaceTime'])},
            'RaceSpeed': {'N': str(data_item['RaceSpeed'])},
            'TimeStamp': {'S': str(data_item['TimeStamp'])},
            'Rank': {'N': str(data_item['Rank'])},
            'Bib': {'S': str(data_item['Bib'])},
            'UCIID': {'N': str(data_item['UCIID'])},
            'FirstName': {'S': str(data_item['FirstName'])},
            'LastName': {'S': str(data_item['LastName'])},
            'ShortTVName': {'S': str(data_item['ShortTVName'])},
            'Team': {'S': str(data_item['Team'])},
            'NOC': {'S': str(data_item['NOC'])},
            'Status': {'S': str(data_item['Status'])},
            'Laps': {'N': str(data_item['Laps'])},
        }

    if data_type == "classification":
        ddb_item = {
            'pk': {'S': 'RIDER#UCIID=' + str(data_item['UCIID']) + '#'},
            'sk': {'S': 'CLASSIFICATION#EventID=' + str(data_item['EventID']) + '#' + 'League=' + str(
                data_item['League']).replace(" ", "") + '#' + 'RaceType=' + str(data_item['RaceType']).replace(" ",
                                                                                                               "") + '#'},
            'Message': {'S': str(data_item['Message'])},
            'SeasonID': {'N': str(data_item['SeasonID'])},
            'EventID': {'N': str(data_item['EventID'])},
            'Gender': {'S': str(data_item['Gender'])},
            'RaceType': {'S': str(data_item['RaceType'])},
            'League': {'S': str(data_item['League'])},
            'State': {'S': str(data_item['State'])},
            'TimeStamp': {'S': str(data_item['TimeStamp'])},
            'Rank': {'N': str(data_item['Rank'])},
            'Bib': {'S': str(data_item['Bib'])},
            'UCIID': {'N': str(data_item['UCIID'])},
            'FirstName': {'S': str(data_item['FirstName'])},
            'LastName': {'S': str(data_item['LastName'])},
            'ShortTVName': {'S': str(data_item['ShortTVName'])},
            'Team': {'S': str(data_item['Team'])},
            'NOC': {'S': str(data_item['NOC'])},
            'Status': {'S': str(data_item['Status'])},
            'Points': {'N': str(data_item['Points'])},
        }

    if data_type == "rounds":
        ddb_item = {
            'pk': {'S': 'EVENT#EventID=' + str(data_item['EventID']) + '#'},
            'sk': {'S': 'ROUNDS#League=' + str(data_item['League']).replace(' ', '') + '#RaceType=' + str(data_item['RaceType']) + '#Round=' + str(data_item['Round']) + '#'},
            'Message': {'S': "Rounds"},
            'SeasonID': {'N': str(data_item['EventID'])[:4]},
            'EventID': {'N': str(data_item['EventID'])},
            'League': {'S': str(data_item['League'])},
            'RaceType': {'S': str(data_item['RaceType'])},
            'Round': {'N': str(data_item['Round'])},
            'StartTime': {'S': str(data_item['StartTime'])},
            'RoundName': {'S': str(data_item['RoundName'])},
            'Duration': {'N': str(data_item['Duration'])},
            'NoOfHeatsInRound': {'S': str(data_item['NoOfHeatsInRound'])},
            'RacesInRound': {'L': data_item['RacesInRound']},
        }
    return ddb_item


def validate_data_input(api_path, record):
    '''
    Checks for missing or additional fields in a record and it's nested subrecords
    :param str api_path: api path
    :param str record: body of the api call (Live or race data)
    :return: return string telling which fields are missing or additional
    '''
    return_str = ''
    field_names = []
    if api_path == '/StorePreSeasonQuestionnaire':
        field_names = ['FirstName', 'LastName', 'BirthDate', 'UCIID', 'Gender',
                       'TrainingLocation', 'LeagueCat', 'Nationality', 'SeasonTitle', 'HeightCm',
                       'WeightKg', 'RestHrBpm', 'MaxHrBpm', 'Flying200', 'GearingForFlying200',
                       'PowerPeakW', 'Power5sW', 'Power15sW', 'Power30sW', 'Power60sW', 'Power120sW',
                       'Power180sW', 'Power300sW', 'Power600sW', 'Power1200sW', 'Power1800sW',
                       'Power3600sW']
        nested_fieldnames_check = False
    elif api_path == '/StoreRacesList':
        field_names = ['Message', 'SeasonID', 'EventID', 'TimeStamp', 'Date',
                       'EventName', 'Races']
        nested_fieldnames_check = True
        nested_fieldnames = {'Races': ['RaceID', 'RaceType', 'Gender', 'League', 'Heat', 'TotalHeats',
                                       'Round', 'TotalRounds', 'Laps', 'Distance', 'RaceName', 'StartTime']}
    elif api_path == '/StoreRaceStartList':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'Gender', 'RaceType', 'League', 'Heat', 'TotalHeats',
                       'Round', 'TotalRounds', 'RaceName', 'Laps', 'Distance', 'TimeStamp', 'Startlist']
        nested_fieldnames_check = True
        nested_fieldnames = {'Startlist': ['Bib', 'UCIID', 'FirstName', 'LastName',
                                           'ShortTVName', 'Team', 'NOC', 'Status', 'StartPosition', 'StartingLane']}
    elif api_path == '/StoreStartTime':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'TimeStamp']
        nested_fieldnames_check = False
    elif api_path == '/StoreRaceStartLive':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'TimeStamp']
        nested_fieldnames_check = False
    elif api_path == '/StoreLapCounter':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'LapsToGo',
                       'DistanceToGo', 'TimeStamp']
        nested_fieldnames_check = False
    elif api_path == '/StoreRiderEliminated':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'RaceName',
                       'TimeStamp', 'Bib', 'UCIID', 'FirstName', 'LastName', 'ShortTVName',
                       'Team', 'NOC']
        nested_fieldnames_check = False
    elif api_path == '/StoreFinishTime':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'TimeStamp', 'RaceTime', 'RaceSpeed']
        nested_fieldnames_check = False
    elif api_path == '/StoreRaceResults':
        field_names = ['Message', 'SeasonID', 'EventID', 'RaceID', 'RaceType', 'Gender', 'League', 'Heat', 'TotalHeats',
                       'Round', 'TotalRounds', 'State', 'RaceName', 'Laps', 'Distance', 'RaceTime',
                       'RaceSpeed', 'TimeStamp', 'Results']
        nested_fieldnames_check = True
        nested_fieldnames = {'Results': ['Rank', 'Bib', 'UCIID', 'FirstName', 'LastName',
                                         'ShortTVName', 'Team', 'NOC', 'Status', 'Laps']}
    elif api_path == '/StoreClassification':
        field_names = ['Message', 'SeasonID', 'EventID', 'Gender', 'RaceType', 'League', 'State', 'TimeStamp',
                       'Results']
        nested_fieldnames_check = True
        nested_fieldnames = {'Results': ['Rank', 'Bib', 'UCIID', 'FirstName', 'LastName',
                                         'ShortTVName', 'Team', 'NOC', 'Status', 'Points']}
    elif api_path == '/StoreLiveRidersTracking':
        field_names = ['Message', 'TimeStamp', 'SeasonID', 'EventID', 'RaceID', 'Captures']
        nested_fieldnames_check = True
        nested_fieldnames = {'Captures': ['TimeStamp', 'Bib', 'UCIID', 'Rank', 'State',
                                          'Distance', 'DistanceProj', 'Speed', 'SpeedMax', 'SpeedAvg', 'DistanceFirst',
                                          'DistanceNext', 'Acc', 'Pos_Lat', 'Pos_Lng']}
    elif api_path == '/StoreLiveRidersData':
        field_names = ['Message', 'TimeStamp', 'SeasonID', 'EventID', 'RaceID', 'Captures']
        nested_fieldnames_check = True
        nested_fieldnames = {'Captures': ['TimeStamp', 'Bib', 'UCIID', 'Heartrate', 'Cadency', 'Power']}
    else:
        logger.error("Check the RaceData endpoint you are hitting")
        return 'Check URL/endpoint'

    if field_names:
        record = flatten_dict(record)
        return_str = check_missing_additional_fields(field_names, list(record.keys()), False)
        # no need to check for nested fields if unnested fields are already incorrect
        if not return_str:
            # check nested fields
            if nested_fieldnames_check:
                for k, v in nested_fieldnames.items():
                    sub_records = record[k]
                    for sr in sub_records:
                        sr = flatten_dict(sr)
                        return_str = check_missing_additional_fields(v, list(sr.keys()), True)
                        if return_str:
                            break
                    if return_str:
                        break

    return return_str


def check_missing_additional_fields(true_fields, actual_fields, nested_fieldnames_check):
    '''
    Checks for missing or additional fields in a record or nested field
    :param list true_fields: fields that should be present in the record
    :param list actual_fields: fields that are present in the current record
    :param boolean nested_fieldnames_check: boolean telling whether it's a nested record or not
    :return: return string telling which fields are missing or additional
    '''
    return_str = ''

    # print nested to indicate whether the wrong field is in the nested record or not
    if nested_fieldnames_check:
        nested_str = 'nested '
    else:
        nested_str = ''

    # fields missing from the record
    missing_fields = list(set(true_fields) - set(actual_fields))
    # additional fields in the record
    additional_fields = list(set(actual_fields) - set(true_fields))

    # create result string
    if len(true_fields) != len(actual_fields):
        return_str = return_str + f"Number of fields in {nested_str}record received does not match to expected number of fields. Expected {len(true_fields)} but received {len(actual_fields)} fields.\n"
    if missing_fields:
        return_str = return_str + f"Missing fields in {nested_str}record: {', '.join(missing_fields)}.\n"
    if additional_fields:
        return_str = return_str + f"Additional fields in {nested_str}record: {', '.join(additional_fields)}.\n"

    if return_str:
        return_str = return_str + f"Expected {true_fields} but received {actual_fields}."

    return return_str


def flatten_dict(d, parent_key='', sep='_'):
    '''
    Flattens a nested dict
    :param dict d: dictionary to flatten
    :param str parent_key: parent of the current dictionary
    :param str sep: seperator of the dictionary hierarchy
    :return: flattened dictionary
    '''
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def append_predicted_rank(race_start_list):
    try:
        elo_means_list = {
            startlist["UCIID"]: float(query_ddb_for_favourite_rider_data(DYNAMODB_TABLE, str(startlist["UCIID"]))) for
            startlist in race_start_list["Startlist"]}
        elo_ranks_list = {key: rank for rank, key in
                          enumerate(sorted(elo_means_list, key=elo_means_list.get, reverse=True), 1)}

        logger.debug(
            "Elo ranking list for Race ID: " + str(race_start_list.get("RaceID", "")) + " is " + str(elo_ranks_list))

        for startlist in race_start_list["Startlist"]:
            # No elo scores found
            if all(means==-1.0 for means in elo_means_list.values()):
                race_start_list["Startlist"][race_start_list["Startlist"].index(startlist)]["PredictedRank"] = 0
            else:
                race_start_list["Startlist"][race_start_list["Startlist"].index(startlist)]["PredictedRank"] = \
                elo_ranks_list[startlist["UCIID"]]
        return race_start_list
    except Exception as ex:
        logger.warning("Failed to compute rider predictions for Race ID: %s", str(race_start_list.get("RaceID", "")))
        logger.warning(ex, exc_info=True)
        return race_start_list


def query_ddb_for_favourite_rider_data(table_name, uciid):
    try:
        response_fav_rider = dynamodb_client.query(
            TableName=table_name,
            KeyConditionExpression='pk = :myPK AND sk = :mySK',
            ExpressionAttributeValues={
                ':myPK': {'S': 'RIDER#UCIID=' + str(uciid) + '#'},
                ':mySK': {"S": 'FAVORITE#'}
            }
        )
        logger.debug("in query_ddb response for FAVORITE#: " + str(response_fav_rider["Items"]))
        return response_fav_rider["Items"][0]["EloMean"]["N"]
    except Exception as ex:
        logger.warning("Failed to find elo scores for UCIID: %s", str(uciid))
        logger.warning(ex, exc_info=True)
        return "-1.0"


def compute_est_timeslot(time_actual_race, time_next_race):
    '''
    Used for Rounds, triggered with RaceList object call
    In order to create Rounds object we have calculate duration of each round.
    In case of last round it assumes 10 minutes, as we don't have next object.
    :param str time_actual_race: timestamp of actual race in format date_format_str
    :param str time_next_race: timestamp of next race in format date_format_str
    :return: int, with rounded (floor) duration of Round
    '''
    date_format_str = '%Y-%m-%dT%H:%M:%S.%fZ'
    if time_next_race == None:
        result = 10
    else:
        diff = datetime.datetime.strptime(time_next_race, date_format_str) - datetime.datetime.strptime(
            time_actual_race, date_format_str)
        result = int(diff.total_seconds() / 60)
    return result


def generate_and_save_round_level_object(race_list):
    '''
    Used for Rounds, triggered with RaceList object call
    In order to create Rounds object we have to decode round number to actual business meaning, form Objects
    Then it puts Rounds Objects to dynamoDB
    :param dict race_list: list with races
    :return: nothing, just execute
    '''
    d = {
        'Sprint': {
            1: {'roundName': 'Eliminations', 'defaultNoOfRiders': 3},
            2: {'roundName': 'Semi finals', 'defaultNoOfRiders': 3},
            3: {'roundName': 'Finals', 'defaultNoOfRiders': 2}
        },
        'Elimination': {
            1: {'roundName': '', 'defaultNoOfRiders': 18},
        },
        'Scratch': {
            1: {'roundName': '', 'defaultNoOfRiders': 18},
        },
        'Keirin': {
            1: {'roundName': 'Qualifications', 'defaultNoOfRiders': 6},
            2: {'roundName': 'Final', 'defaultNoOfRiders': 6},
        }
    }

    # select only races with 1st Heat, as it is start of round.
    round_start_races = []
    for x in race_list:
        if x['Heat'] == 1:
            x['RacesInRound'] = []
            round_start_races.append(x)

    # Put all races which are within scope of round/league. Source data might not be in order, hence we iterate through all list.
    for round_start_race in round_start_races:
        for race in race_list:
            if round_start_race['RaceType'] == race['RaceType'] and round_start_race['Round'] == race['Round'] and round_start_race['League'] == race['League']:
                round_start_race['RacesInRound'].append({"M": {'RaceID': {'N': str(race['RaceID'])},
                                                         'NoOfRiders': {'N': str(d.get(race['RaceType']).get(race['Round']).get(
                                                             'defaultNoOfRiders'))},
                                                         'Heat': {'N': str(race['Heat'])}}})

    # sort by TimeStamp in ascending order, to get proper order
    def get_start_time(round_object):
        return round_object.get('StartTime')

    round_start_races.sort(key=get_start_time)

    rounds = []
    order = 1
    # we have to calculate duration of Round, so we need current and next object
    for x, x1 in zip(round_start_races, round_start_races[1:] + [None]):
        if x1 == None:
            x1 = {}
        start_time = datetime.datetime.strptime(x['StartTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
        obj = {
            'EventID': str(x['RaceID'])[:6],
            'Order': order,
            'League': x['League'],
            'Gender': x['Gender'],
            'RaceType': x['RaceType'],
            'Round': x['Round'],
            'StartTime': datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S.%f')[:-3],
            'Duration': compute_est_timeslot(x['StartTime'], x1.get('StartTime')),
            'RoundName': x['RaceType'] + " " + x['Gender'] + " " + d.get(x['RaceType']).get(x['Round']).get('roundName'),
            'NoOfHeatsInRound': x['TotalHeats'],
            'RacesInRound': x['RacesInRound']
        }
        rounds.append(obj)
        order += 1

    for round in rounds:
        logger.info(str(round))
        ddb_item = convert_record_to_ddb_item(round, "rounds")
        logger.info(str(ddb_item))
        save_item_to_ddb(DYNAMODB_TABLE, ddb_item)
    logger.info("Successfully generated and saved %s Rounds objects under %s# ddb pk" % (len(rounds), "EVENT#EventID=" + str(x['RaceID'])[:6]))

def update_round_level_object_with_startlist(start_list):
    '''
    Used for Rounds, triggered with StartList object call
    In order to enrich Rounds object with RacesInRound we have to first get actual state of function,
    Then append new race which is within scope of round and update Round object
    :param dict start_list: name of table
    :return: nothing, just execute
    '''
    count = 0
    UCIIDs = []
    for rider in start_list['Startlist']:
        count += 1
        UCIIDs.append(rider['UCIID'])

    race_summary = {"M": {
        'RaceID': {"N": str(start_list['RaceID'])},
        'RidersUCIID': {"L": (list(map(lambda riderid: {"N": str(riderid)}, UCIIDs)))},
        'NoOfRiders': {"N": str(count)},
        'Heat': {"N": str(start_list['Heat'])}}
    }
    # get current version of Round object
    actual = query_ddb_for_round_object(DYNAMODB_TABLE, start_list['EventID'], start_list['League'], start_list['RaceType'], start_list['Round'])

    # Deletes the race in RacesInRound which matches with the one form StartList
    # and appends the updated race_summary based on Startlist to the rest of races
    # Only one RaceID with one heat number (1,2,3) in the same round, so won't match races across rounds with same heat number
    new_races_in_rounds = [x for x in actual['RacesInRound']['L'] if not (x['M']['RaceID']['N'] == race_summary['M']['RaceID']['N'] and x['M']['Heat']['N'] == race_summary['M']['Heat']['N'])]
    new_races_in_rounds.append(race_summary)
    actual['RacesInRound']['L'] = new_races_in_rounds

    update_params = {
            'keys': {
                        'pk': {"S": actual['pk']['S']},
                        'sk': {"S": actual['sk']['S']}},
            'update_expressions': 'SET RacesInRound = :newRacesInRound',
            'expression_values': {':newRacesInRound':  actual['RacesInRound']}
        }
    update_ddb_item(DYNAMODB_TABLE, update_params['keys'], update_params['update_expressions'], update_params['expression_values'])
    logger.info("Successfully updated Round objects in ddb under pk=%s  sk=%s  " % (actual['pk']['S'], actual['sk']['S']))



def update_ddb_item(_table_name, _keys, _update_expressions, _expression_values, _retry_count=3):
    '''
    Used for Rounds, triggered with StartList object call
    Generic functiion - Updates dynamoDB item
    :param str _table_name: name of table
    :param dict _keys: keys for item to update
    :param str _update_expressions: update commands to execute on dynamodb side
    :param str _expression_values: populated values to variables used in _update_expressions
    :param int _retry_count: how many times shall we retry in case of failure
    :return: nothing, just execute
    '''
    try:
        dynamodb_client.update_item(TableName=_table_name,
                                    Key=_keys,
                                    UpdateExpression=_update_expressions,
                                    ExpressionAttributeValues=_expression_values,
                                    ReturnValues="UPDATED_NEW"
                                    )
    except Exception as ex:
        logger.error("Failed to update dynamodb fields with keys: %s try number: %s" % (_keys, _retry_count))
        logger.error(ex, exc_info=True)
        retry_count = _retry_count - 1
        if retry_count > 0:
            update_ddb_item(_table_name, _keys, _update_expressions, _expression_values, retry_count)


def query_ddb_for_round_object(table_name, event_id, league, race_type, round_no, _retry_count=3):
    '''
    Used for Rounds, triggered with StartList object call
    Specific purpose function - Returns Rounds object from DynamoDB for race
    :param str _table_name: name of table
    :param dict event_id: EventID like 202101
    :param str league: League of Race
    :param str race_type: RaceType of Race
    :param str round_no: Round number
    :return: 1st entry with Rounds object
    '''
    response = None
    try:
        response = dynamodb_client.query(
            TableName=table_name,
            KeyConditionExpression='pk = :myPK and begins_with(sk, :mySk)',
            ExpressionAttributeValues={
                ':myPK': {'S': 'EVENT#EventID='+ str(event_id) + '#' },
                ':mySk': {"S": 'ROUNDS#League=' + str(league).replace(" ", "") + "#RaceType=" + str(race_type) + '#Round=' + str(round_no) + '#'}
            }
        )["Items"][0]
    except Exception as ex:
        pk = 'EVENT#EventID='+ str(event_id) + '#'
        sk = 'ROUNDS#League=' + str(league).replace(" ", "") + "#RaceType=" + str(race_type) + '#Round=' + str(round_no) + '#'
        logger.error("Cannot find Round in RaceList or Rounds: pk=%s ; sk=%s try number: %s" % (pk, sk, _retry_count))
        logger.error(ex, exc_info=True)
        retry_count = _retry_count - 1
        if retry_count > 0:
            query_ddb_for_round_object(table_name, event_id, league, round_no, retry_count)
    return response
