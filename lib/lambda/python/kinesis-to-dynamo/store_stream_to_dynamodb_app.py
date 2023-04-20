from __future__ import print_function

import base64
import datetime
import logging
import os
from json import loads

import boto3

dynamodb_client = boto3.client('dynamodb')
cloudwatch_client = boto3.client('cloudwatch')
ses_client = boto3.client('ses')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

table_name_live = os.environ['DYNAMODB_TABLE_NAME_LIVE']
table_name_static = os.environ['DYNAMODB_TABLE_NAME_STATIC']
project_env = os.environ['ENVIRONMENT']
NUMBER_OF_RETRIES = 2

# ONLY FOR PERSONAL BEST
SENDER = os.environ['SENDER']
RECIPIENT = os.environ['RECIPIENT']
CC = os.environ['CC'] # REMOVE FOR PROD
SUBJECT = project_env + "- Personal Best Notification Race: "
BODY_TEXT = ""
CHARSET = "UTF-8"
POWER_FIELDS = ["Power5s", "Power15s", "Power30s", "Power60s", "Power120s", "Power180s", "Power300s", "Power600s"]

def lambda_handler(event, context):
    payload = event['records']
    logger.info('Got : %s Records', len(payload))
    output = []
    success = 0
    failure = 0
    put_request_list = []
    update_request_list_riders_data = []
    update_request_list_riders_tracking = []
    records_processed_in_current_batch = []
    metrics_for_cw = {}
    number_of_elements_to_process = len(payload)
    logger.info("Number of records to process: {}".format(number_of_elements_to_process))
    batch_processing_start = time_now_str()
    for i, record in enumerate(payload):
        try:
            # main flow which handles standard items and raw messages
            ddb_item = convert_event_to_dbd_item(record)

            records_processed_in_current_batch.append({"record": record, "ddb_item": ddb_item})
            logger.debug('ddb_item: %s', ddb_item)
            put_request = {
                "PutRequest": {
                    "Item": ddb_item
                }
            }
            put_request_list.append(put_request)
            # batch every 25 elements or process remaining items if less then 25
            try:
                metrics_for_cw[ddb_item["Message"]['S']] = {}
                metrics_for_cw[ddb_item["Message"]['S']]['APIIngestTime'] = ddb_item["ApiIngestTime"]['S']
                metrics_for_cw[ddb_item["Message"]['S']]['KinesisAnalyticsIngestTime'] = ddb_item["KinesisAnalyticsIngestTime"]['S']
                metrics_for_cw[ddb_item["Message"]['S']]['DynamoIngestTime'] = ddb_item["DynamoIngestTime"]['S']
                metrics_for_cw[ddb_item["Message"]['S']]['StoreToDynamoLambdaTimeStart'] = batch_processing_start
            except Exception as e:
                logger.error(e, exc_info=True)
                logger.error("Failed to get metrics from record.")

            if len(put_request_list) == 25 or i == number_of_elements_to_process - 1:
                try:
                    logger.debug("Processed {} number of records. Sending them to DynamoDB table.".format(len(put_request_list)))
                    logger.debug("items before distinct: {}".format(put_request_list))
                    distinct_put_requests = list({str(v['PutRequest']['Item']['pk']) + str(v['PutRequest']['Item']['sk']): v for v in put_request_list}.values())
                    logger.info("Number of distinct entries stored to DynamoDB {}".format(len(distinct_put_requests)))
                    data = {
                        table_name_live: distinct_put_requests
                    }
                    logger.debug("put_requests: " + str(data))
                    multi_put_result = dynamodb_client.batch_write_item(RequestItems=data)
                    logger.debug("multi put result: " + str(multi_put_result))
                    unprocessed_items = multi_put_result.get("UnprocessedItems").get(table_name_live, "")
                    if len(unprocessed_items) > 0:
                        # In case there are unprocessed items try sending them again (see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
                        # We try it once and assume succeeds. This will happen if any requested operations fail because the table's provisioned
                        # throughput is exceeded or an internal processing failure occurs
                        logger.warning("The following records were not processed: {}. Trying again.".format(unprocessed_items))
                        multi_put_result = dynamodb_client.batch_write_item(RequestItems=multi_put_result.get("UnprocessedItems"))
                        logger.info("Retry result: {}.".format(multi_put_result))
                    success += len(records_processed_in_current_batch)
                    for currently_processed_record in records_processed_in_current_batch:
                        output.append({'recordId': currently_processed_record.get("record")['recordId'], 'result': 'Ok'})
                    put_request_list.clear()
                    records_processed_in_current_batch.clear()
                    for key in metrics_for_cw.keys():
                        metrics_for_cw.get(key)['StoreToDynamoLambdaTimeEnd'] = time_now_str()
                    put_timing_stats_to_cw(metrics_for_cw)
                except Exception as e:
                    logger.error(e, exc_info=True)
                    logger.error("Failed to store batch of records. Trying to put then one by one")
                    # In case of batch exception try to process individual records one by one
                    for currently_processed_record in records_processed_in_current_batch:
                        try:
                            put_item_to_ddb(currently_processed_record.get("ddb_item"), table_name_live)
                            success += 1
                        except Exception as e:
                            logger.error(e, exc_info=True)
                            logger.error("Failed to process single record.")
                            if (currently_processed_record.get("record")['lambdaDeliveryRecordMetadata']['retryHint'] > NUMBER_OF_RETRIES):
                                # TODO: implement dead letter queue for such records
                                logger.info("skipping record after %d retries", currently_processed_record['lambdaDeliveryRecordMetadata']['retryHint'])
                                output.append({'recordId': currently_processed_record.get("record")['recordId'], 'result': 'Ok'})
                            else:
                                output.append({'recordId': currently_processed_record.get("record")['recordId'], 'result': 'DeliveryFailed', 'reason:': str(e)})
                                failure += 1
                                output.append({'recordId': currently_processed_record.get("record")['recordId'], 'result': 'Ok'})
                    for key in metrics_for_cw.keys():
                        metrics_for_cw.get(key)['StoreToDynamoLambdaTimeEnd'] = time_now_str()
                    put_timing_stats_to_cw(metrics_for_cw)
                    put_request_list.clear()
                    records_processed_in_current_batch.clear()

            race_details = extract_message_details(record)
            logger.debug("Race details: " + str(race_details))

            # side computations which populates derived data like aggregates, updates for race_status
            # or LATEST row for each rider with the most current information.
            if race_details["InputMessage"] in ["AggRidersData", "AggRidersTracking"]:
                # updates current rider item
                logger.debug("Updating Rider #LATEST item")
                update_params = get_update_params_for_latest(ddb_item)
                logger.debug("update params: {}".format(update_params))
                transact_item = {
                    "Update": {
                        'TableName': table_name_live,
                        'Key': update_params['keys'],
                        'UpdateExpression': update_params['update_expressions'],
                        "ExpressionAttributeValues": update_params['expression_values']

                    }
                }
                if race_details["InputMessage"] == "AggRidersData":
                    update_request_list_riders_data.append({
                        "record": record,
                        "transact_item": transact_item,
                        "update_params": update_params
                    })
                if len(update_request_list_riders_data) == 25:
                    update_agg_latest(update_request_list_riders_data)
                if race_details["InputMessage"] == "AggRidersTracking":
                    update_request_list_riders_tracking.append({
                        "record": record,
                        "transact_item": transact_item,
                        "update_params": update_params
                    })
                if len(update_request_list_riders_tracking) == 25:
                    update_agg_latest(update_request_list_riders_tracking)
            # if some aggregates are left to process  and the whole batch was processed update aggreagates
            if i == number_of_elements_to_process - 1 and len(update_request_list_riders_data) > 0:
                update_agg_latest(update_request_list_riders_data)
            if i == number_of_elements_to_process - 1 and len(update_request_list_riders_tracking) > 0:
                update_agg_latest(update_request_list_riders_tracking)
            elif race_details["InputMessage"] == "RaceStartLive":
                # set race_status to LIVE
                update_params = get_update_params_for_race_status(ddb_item, new_race_status="LIVE")
                update_ddb_item(table_name_static, _keys=update_params['keys'],
                                _update_expressions=update_params['update_expressions'],
                                _expression_values=update_params['expression_values'])
            elif race_details["InputMessage"] == "FinishTime":
                # compute race aggregates like AGG#RaceStats# or AGG#RiderStats#
                # set race_status to FINISHED
                store_aggregates(race_details)

                logger.info("Updating Race Status to FINISHED")
                update_params = get_update_params_for_race_status(ddb_item, new_race_status="FINISHED")
                update_ddb_item(table_name_static, _keys=update_params['keys'],
                                _update_expressions=update_params['update_expressions'],
                                _expression_values=update_params['expression_values'])
                # trigger personal best email notification after race end
                trigger_personal_best(table_name_live, table_name_static, race_details)

        except Exception as e:
            logger.error(e, exc_info=True)
    logger.info('Successfully delivered %s records, failed to deliver %s records', success, failure)
    return {'records': output}


def update_agg_latest(update_request_list):
    try:
        # get distinct updates for any given key (only one update for a given primary key is allowed in one batch)
        # order list by timestamp
        def get_start_time(round_object):
            return round_object['transact_item']['Update']['Key']['sk']['S']
        update_request_list.sort(key=get_start_time)
        transact_items = list({str(v['transact_item']['Update']['Key']): v['transact_item'] for v in update_request_list}.values())
        logger.info("Number of unique updates to perform: {}".format(len(transact_items)))
        update_result = dynamodb_client.transact_write_items(TransactItems=transact_items)
        update_request_list.clear()
    except Exception as e:
        logger.error("Failed to update in batch")
        logger.error(e, exc_info=True)
        logger.info("Trying to process updates one by one. ")
        for update in update_request_list:
            update_params = update.get("update_params")
            update_ddb_item(table_name_live, _keys=update_params['keys'],
                            _update_expressions=update_params['update_expressions'],
                            _expression_values=update_params['expression_values'])
        update_request_list.clear()


def store_aggregates(race_details):
    try:
        logger.info("Storing aggregates.")
        logger.debug("Querying for race details")
        race_results = query_ddb_for_live_riders_tracking_and_data_agg(table_name_live, race_details["RaceID"])
        if len(race_results["Items"]) != 0:
            logger.debug("Converting query results to DynamoDB record")
            aggregates = create_dbd_item_from_query_results_for_race(race_results)
            logger.debug("calculated aggregates: %s", str(aggregates))
            put_item_to_ddb(aggregates, table_name_live)
        else:
            logger.error("Check live data stream. No aggregate data has been computed for RaceID: " + str(race_details["RaceID"]))
    except Exception as ex:
        logger.error("failed to calculate and store race and riders in race level aggregates")
        logger.error(ex, exc_info=True)


def get_update_params_for_latest(original_ddb_item: dict):
    '''
    Response update parameters for object_name based on our scenario
    After getting AggLiveRiders or AggLiveRidersTracking - it sets the #LATEST row in live table: pk=AGG#JOINEDLIVEDATA#LATEST#
    :param dict original_ddb_item: it is generated schema prepared for insertion of original object
    :return: dict update parameters which are used in
    '''
    update_parameters = {
        'keys': {
            'pk': {"S": 'AGG#JOINEDLIVEDATA#LATEST#'},
            'sk': {"S": 'RACE#RaceID=' + original_ddb_item['RaceID']['S'] + '#' + "UCIID=" + original_ddb_item['UCIID']['S'] + '#'}},
        'update_expressions': 'SET EventTimeStamp = :newEventTimeStamp ',
        'expression_values': {':newEventTimeStamp': original_ddb_item['EventTimeStamp']}
    }

    logger.debug("original_ddb_item.items(): {}".format(str(original_ddb_item.items())))

    # we iterate over all elements in original aggregates and generate set operations
    for key in original_ddb_item.keys():
        logger.debug("key: {}".format(str(key)))
        if key in ["pk", "sk", "InputMessage", "EventTimeStamp"]:
            continue
        update_parameters['update_expressions'] = update_parameters['update_expressions'] + ', ' + key + ' = :new' + key
        update_parameters['expression_values'][':new' + key] = original_ddb_item[key]

    return update_parameters


def get_update_params_for_race_status(original_ddb_item: dict, new_race_status="SCHEDULED", ):
    '''
    Response update parameters for object_name based on our scenario
    :param dict original_ddb_item: it is generated schema prepared for insertion of original object
    :param str new_race_status: status of race - populate only when object_name==race_status
    :return: dict update parameters which are used in
    '''
    update_parameters = {
        'comment': 'After getting Start, Finish sing - it sets the new RaceID in static table: pk=EVENT#EventID=',
        'keys': {
            'pk': {"S": 'EVENT#EventID=' + original_ddb_item['EventID']['S'] + '#'},
            'sk': {"S": 'RACE#RaceID=' + original_ddb_item['RaceID']['S'] + '#'}},
        'update_expressions': 'SET RaceStatus = :newRaceStatus',
        'expression_values': {':newRaceStatus': {"S": new_race_status}}
    }
    return update_parameters


def update_ddb_item(_table_name, _keys, _update_expressions, _expression_values, _retry_count=3):
    '''
    Updates dynamoDB item
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


def put_item_to_ddb(ddb_item, _table_name):
    dynamodb_client.put_item(TableName=_table_name, Item=ddb_item)


def extract_message_details(record):
    logger.debug('record: %s', record)
    logger.debug('record[data]: %s', record['data'])
    payload = base64.b64decode(record['data'])
    logger.debug('base64 decoded payload: %s', payload)
    data_item = loads(payload)
    input_message = data_item['InputMessage']
    logger.debug('Converting message %s', input_message)
    details = {
        'InputMessage': data_item.get('InputMessage', ""),
        'UCIID': str(data_item.get('UCIID', "")),
        'SeasonID': str(data_item.get('SeasonID', "")),
        'EventID': str(data_item.get('EventID', "")),
        'RaceID': str(data_item.get('RaceID', ""))
    }
    return details


def convert_event_to_dbd_item(record):
    logger.debug('record: %s', record)
    logger.debug('record[data]: %s', record['data'])
    payload = base64.b64decode(record['data'])
    logger.debug('base64 decoded payload: %s', payload)
    data_item = loads(payload)
    input_message = data_item['InputMessage']
    logger.debug('Converting message %s', input_message)

    output_schema = {
        'LiveRidersTracking': {
            'pk': {'S': "LIVERIDERSTRACKING#RaceID=" + str(data_item.get('RaceID', "")) + "#"},
            'sk': {
                'S': "UCIID=" + str(data_item.get('UCIID', "")) + "#EventTimeStamp=" + data_item.get('EventTimeStamp', "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'EventTimeStamp': {'S': data_item.get('EventTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'Bib': {'S': str(data_item.get('Bib', ""))},
            'UCIID': {'S': str(data_item.get('UCIID', ""))},
            'RiderRank': {'N': str(data_item.get('RiderRank'))},
            'State': {'S': str(data_item.get('State', ""))},
            'Distance': {'N': str(data_item.get('Distance'))},
            'DistanceProj': {'N': str(data_item.get('DistanceProj'))},
            'Speed': {'N': str(data_item.get('Speed'))},
            'SpeedMax': {'N': str(data_item.get('SpeedMax'))},
            'SpeedAvg': {'N': str(data_item.get('SpeedAvg'))},
            'DistanceFirst': {'N': str(data_item.get('DistanceFirst'))},
            'DistanceNext': {'N': str(data_item.get('DistanceNext'))},
            'Acc': {'N': str(data_item.get('Acc'))},
            'Lat': {'N': str(data_item.get('Lat'))},
            'Lng': {'N': str(data_item.get('Lng'))}
        },
        'LiveRidersData': {
            'pk': {'S': "LIVERIDERSDATA#RaceID=" + str(data_item.get('RaceID', "")) + "#"},
            'sk': {
                'S': "UCIID=" + str(data_item.get('UCIID', "")) + "#EventTimeStamp=" + data_item.get('EventTimeStamp',
                                                                                                     "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'EventTimeStamp': {'S': data_item.get('EventTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'Bib': {'S': str(data_item.get('Bib', ""))},
            'UCIID': {'S': str(data_item.get('UCIID', ""))},
            'RiderHeartrate': {'N': str(data_item.get('RiderHeartrate'))},
            'RiderCadency': {'N': str(data_item.get('RiderCadency', ""))},
            'RiderPower': {'N': str(data_item.get('RiderPower'))}
        },
        'StartTime': {
            'pk': {'S': "STARTTIME#"},
            'sk': {'S': "RaceID=" + str(data_item.get('RaceID', "")) + "#EventTimeStamp=" + data_item.get('ServerTimeStamp',
                                                                                                          "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
        },
        'RaceStartLive': {
            'pk': {'S': "RACESTARTLIVE#"},
            'sk': {'S': "RaceID=" + str(data_item.get('RaceID', "")) + "#EventTimeStamp=" + data_item.get(
                'ServerTimeStamp',
                "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
        },
        'FinishTime': {
            'pk': {'S': "FINISHTIME#"},
            'sk': {'S': "RaceID=" + str(data_item.get('RaceID', "")) + "#EventTimeStamp=" + data_item.get('ServerTimeStamp',
                                                                                                          "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'RaceTime': {'S': str(data_item.get('RaceTime', ""))},
            'RaceSpeed': {'S': str(data_item.get('RaceSpeed', ""))}
        },
        'LapCounter': {
            'pk': {'S': "LAPCOUNTER#"},
            'sk': {'S': "RaceID=" + str(data_item.get('RaceID', "")) + "#EventTimeStamp=" + data_item.get('ServerTimeStamp',
                                                                                                          "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'LapsToGo': {'S': str(data_item.get('LapsToGo', ""))},
            'DistanceToGo': {'S': str(data_item.get('DistanceToGo', ""))}
        },
        'RiderEliminated': {
            'pk': {'S': "RIDERELIMINATED#"},
            'sk': {'S': "RaceID=" + str(data_item.get('RaceID', "")) + "#EventTimeStamp=" + data_item.get('ServerTimeStamp',
                                                                                                          "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'ServerTimeStamp': {'S': data_item.get('ServerTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'EliminatedRaceName': {'S': str(data_item.get('EliminatedRaceName', ""))},
            'Bib': {'S': str(data_item.get('Bib', ""))},
            'UCIID': {'S': str(data_item.get('UCIID', ""))},
            'FirstName': {'S': str(data_item.get('FirstName', ""))},
            'LastName': {'S': str(data_item.get('LastName', ""))},
            'ShortTVName': {'S': str(data_item.get('ShortTVName', ""))},
            'Team': {'S': str(data_item.get('Team', ""))},
            'NOC': {'S': str(data_item.get('NOC', ""))}
        },
        'AggRidersData': {
            'pk': {'S': "AGG#LIVERIDERSDATA#RaceID=" + str(data_item.get('RaceID', "")) + "#"},
            'sk': {
                'S': "UCIID=" + str(data_item.get('UCIID', "")) + "#EventTimeStamp=" + data_item.get('EventTimeStamp',
                                                                                                     "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'EventTimeStamp': {'S': data_item.get('EventTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'Bib': {'S': str(data_item.get('Bib', ""))},
            'LeagueCat': {'S': str(data_item.get('LeagueCat', ""))},
            'UCIID': {'S': str(data_item.get('UCIID', ""))},
            'RiderHeartrate': {'N': str(data_item.get('RiderHeartrate', ""))},
            'AvgRaceRiderHeartrate': {'N': str(data_item.get('AvgRaceRiderHeartrate', ""))},
            'MaxRaceRiderHeartrate': {'N': str(data_item.get('MaxRaceRiderHeartrate', ""))},
            'MaxRaceHeartrate': {'N': str(data_item.get('MaxRaceHeartrate', ""))},
            'RiderCadency': {'N': str(data_item.get('RiderCadency', ""))},
            'AvgRaceRiderCadency': {'N': str(data_item.get('AvgRaceRiderCadency', ""))},
            'MaxRaceRiderCadency': {'N': str(data_item.get('MaxRaceRiderCadency', ""))},
            'MaxRaceCadency': {'N': str(data_item.get('MaxRaceCadency', ""))},
            'RiderPower': {'N': str(data_item.get('RiderPower', ""))},
            'AvgRaceRiderPower': {'N': str(data_item.get('AvgRaceRiderPower', ""))},
            'MaxRaceRiderPower': {'N': str(data_item.get('MaxRaceRiderPower', ""))},
            'MaxRacePower': {'N': str(data_item.get('MaxRacePower', ""))},
            'IsInHeartrateRedZone': {'N': str(data_item.get('IsInHeartrateRedZone', ""))},
            'TimeSpentInRedZone': {'N': str(data_item.get('TimeSpentInRedZone', ""))},
            'IsInHeartrateOrangeZone': {'N': str(data_item.get('IsInHeartrateOrangeZone', ""))},
            'TimeSpentInOrangeZone': {'N': str(data_item.get('TimeSpentInOrangeZone', ""))},
            'IsInHeartrateGreenZone': {'N': str(data_item.get('IsInHeartrateGreenZone', ""))},
            'TimeSpentInGreenZone': {'N': str(data_item.get('TimeSpentInGreenZone', ""))}
        },
        'AggRidersTracking': {
            'pk': {'S': "AGG#LIVERIDERSTRACKING#RaceID=" + str(data_item.get('RaceID', "")) + "#"},
            'sk': {
                'S': "UCIID=" + str(data_item.get('UCIID', "")) + "#EventTimeStamp=" + data_item.get('EventTimeStamp',
                                                                                                     "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'EventTimeStamp': {'S': data_item.get('EventTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'UCIID': {'S': str(data_item.get('UCIID', ""))},
            'RiderSpeed': {'N': str(data_item.get('RiderSpeed', ""))},
            'AvgRaceRiderSpeed': {'N': str(data_item.get('AvgRaceRiderSpeed', ""))},
            'MaxRaceRiderSpeed': {'N': str(data_item.get('MaxRaceRiderSpeed', ""))},
            'MaxRaceSpeed': {'N': str(data_item.get('MaxRaceSpeed', ""))},
            'RiderRank': {'N': str(data_item.get('RiderRank', ""))}
        },
        'AggPersonalBest': {
            'pk': {'S': "AGG#PERSONALBEST#RaceID=" + str(data_item.get('RaceID', "")) + "#"},
            'sk': {
                'S': "UCIID=" + str(data_item.get('UCIID', "")) + "#EventTimeStamp=" + data_item.get('EventTimeStamp',
                                                                                                     "") + "#"},
            'Message': {'S': data_item.get('InputMessage', "")},
            'ApiIngestTime': {'S': data_item.get('ApiIngestTime', "")},
            'KinesisAnalyticsIngestTime': {'S': data_item.get('KinesisAnalyticsIngestTime', "")},
            'DynamoIngestTime': {'S': time_now_str()},
            'EventTimeStamp': {'S': data_item.get('EventTimeStamp', "")},
            'SeasonID': {'S': str(data_item.get('SeasonID', ""))},
            'EventID': {'S': str(data_item.get('EventID', ""))},
            'RaceID': {'S': str(data_item.get('RaceID', ""))},
            'Bib': {'S': str(data_item.get('Bib', ""))},
            'LeagueCat': {'S': str(data_item.get('LeagueCat', ""))},
            'UCIID': {'S': str(data_item.get('UCIID', ""))},
            'RiderHeartrate': {'N': str(data_item.get('RiderHeartrate', ""))},
            'RiderHeartrateExceeded': {'N': str(data_item.get('RiderHeartrateExceeded', ""))},
            'RiderPower': {'N': str(data_item.get('RiderPower', ""))},
            'RiderPowerExceeded': {'N': str(data_item.get('RiderPowerExceeded', ""))},
            'Power5s': {'N': str(data_item.get('Power5s', ""))},
            'Power5sExceeded': {'N': str(data_item.get('Power5sExceeded', ""))},
            'Power15s': {'N': str(data_item.get('Power15s', ""))},
            'Power15sExceeded': {'N': str(data_item.get('Power15sExceeded', ""))},
            'Power30s': {'N': str(data_item.get('Power30s', ""))},
            'Power30sExceeded': {'N': str(data_item.get('Power30sExceeded', ""))},
            'Power60s': {'N': str(data_item.get('Power60s', ""))},
            'Power60sExceeded': {'N': str(data_item.get('Power60sExceeded', ""))},
            'Power120s': {'N': str(data_item.get('Power120s', ""))},
            'Power120sExceeded': {'N': str(data_item.get('Power120sExceeded', ""))},
            'Power180s': {'N': str(data_item.get('Power180s', ""))},
            'Power180sExceeded': {'N': str(data_item.get('Power180sExceeded', ""))},
            'Power300s': {'N': str(data_item.get('Power300s', ""))},
            'Power300sExceeded': {'N': str(data_item.get('Power300sExceeded', ""))},
            'Power600s': {'N': str(data_item.get('Power600s', ""))},
            'Power600sExceeded': {'N': str(data_item.get('Power600sExceeded', ""))},
        }
    }.get(input_message, {})

    # removing json fields with null values in numeric columns to do not store it in dynamo.
    cleaned_output_schema = {k: v for k, v in output_schema.items() if
                             (v.get('S') is not None) or (v.get('N') is not None and (v.get('N', "").replace('.', '', 1).isdigit()))}
    logger.debug('cleaned_output_schema %s', cleaned_output_schema)
    return cleaned_output_schema


def create_dbd_item_from_query_results_for_race(results):
    logger.debug('record: %s', results)
    # raceId and league is assumed to be the same for all riders in a race
    race_id = results["Items"][0]["RaceID"]["S"]

    max_cadency = {"UCIID": "-1", "value": 0}
    max_speed = {"UCIID": "-1", "value": 0}
    max_hr = {"UCIID": "-1", "value": 0}
    max_power = {"UCIID": "-1", "value": 0}

    for record in results["Items"]:
        if "MaxRaceRiderCadency" in record and int(record["MaxRaceRiderCadency"]['N']) > int(max_cadency["value"]):
            max_cadency = {"UCIID": record["UCIID"]['S'], "value": record["MaxRaceRiderCadency"]['N']}
        if "MaxRaceRiderHeartrate" in record and int(record["MaxRaceRiderHeartrate"]['N']) > int(max_hr["value"]):
            max_hr = {"UCIID": record["UCIID"]['S'], "value": record["MaxRaceRiderHeartrate"]['N']}
        if "MaxRaceRiderPower" in record and (int(record["MaxRaceRiderPower"]['N']) > int(max_power["value"])):
            max_power = {"UCIID": record["UCIID"]['S'], "value": record["MaxRaceRiderPower"]['N']}
        if "MaxRaceRiderSpeed" in record and int(record["MaxRaceRiderSpeed"]['N']) > int(max_speed["value"]):
            max_speed = {"UCIID": record["UCIID"]['S'], "value": record["MaxRaceRiderSpeed"]['N']}

    output_schemas = {
        # for put_item
        'RaceAggregates': {
            "pk": {'S': "AGG#RaceStats#"},
            "sk": {'S': "RaceID=" + str(race_id) + "#"},
        }
    }
    # TODO: Change to support None type instead of -1, like in cleaned_output_schema in convert_event_to_dbd_item
    if max_power["UCIID"] != "-1":
        output_schemas['RaceAggregates']["max_power"] = {"M": {'UCIID': {'N': str(max_power["UCIID"])}, 'value': {'N': str(max_power["value"])}}}
    if max_hr["UCIID"] != "-1":
        output_schemas['RaceAggregates']["max_hr"] = {"M": {"UCIID": {'N': str(max_hr["UCIID"])}, "value": {'N': str(max_hr["value"])}}}
    if max_speed["UCIID"] != "-1":
        output_schemas['RaceAggregates']["max_speed"] = {"M": {"UCIID": {'N': str(max_speed["UCIID"])}, "value": {'N': str(max_speed["value"])}}}
    if max_cadency["UCIID"] != "-1":
        output_schemas['RaceAggregates']["max_cadency"] = {"M": {"UCIID": {'N': str(max_cadency["UCIID"])}, "value": {'N': str(max_cadency["value"])}}}
    logger.debug('output_schema %s', output_schemas)
    return output_schemas.get('RaceAggregates', {})


def query_ddb_for_live_riders_tracking_and_data_agg(table_name, race_id):
    response_agg_latest = dynamodb_client.query(
        TableName=table_name,
        KeyConditionExpression='pk = :myPK and begins_with(sk, :mySk)',
        ExpressionAttributeValues={
            ':myPK': {'S': 'AGG#JOINEDLIVEDATA#LATEST#'},
            ':mySk': {"S": 'RACE#RaceID=' + race_id + '#'}
        }
    )
    logger.debug("in query_ddb response for AGG#LIVERIDERSDATA: " + str(response_agg_latest["Items"]))
    return response_agg_latest


def time_now_str():
    """
    Generates start date like 2021-08-11 09:52:42.244
    :return  actual timestamp
    """
    return datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S.%f')[:-3]


def put_timing_stats_to_cw(metrics):
    def compute_diff_ms(time1, time2):
        date_format_str = '%Y-%m-%d %H:%M:%S.%f'
        diff = datetime.datetime.strptime(time2, date_format_str) - datetime.datetime.strptime(time1, date_format_str)
        return int(diff.total_seconds() * 1000)


    logger.debug("metrics: {}".format(str(metrics)))
    for key in metrics.keys():
        try:
            entity = key
            api_ingest_time = metrics.get(key)['APIIngestTime']
            kinesis_analytics_ingest_time = metrics.get(key)['KinesisAnalyticsIngestTime']
            dynamo_ingest_time = metrics.get(key)['DynamoIngestTime']
            lambda_ingest_time_start = metrics.get(key)['StoreToDynamoLambdaTimeStart']
            lambda_ingest_time_end = metrics.get(key)['StoreToDynamoLambdaTimeEnd']
            total_pipeline_time_ms = compute_diff_ms(api_ingest_time, dynamo_ingest_time)
            api_to_kinesis_time_ms = compute_diff_ms(api_ingest_time, kinesis_analytics_ingest_time)
            kinesis_to_dynamo_time_ms = compute_diff_ms(kinesis_analytics_ingest_time, dynamo_ingest_time)
            lambda_to_dynamo_time_ms = compute_diff_ms(lambda_ingest_time_start, lambda_ingest_time_end)

            cloudwatch_client.put_metric_data(
                MetricData=[
                    {
                        'MetricName': 'TotalPipelineTimeMs',
                        'Dimensions': [
                            {
                                'Name': 'Env',
                                'Value': project_env
                            },
                            {
                                'Name': 'Entity',
                                'Value': entity
                            },
                        ],
                        'Unit': 'Milliseconds',
                        'Value': total_pipeline_time_ms
                    },
                    {
                        'MetricName': 'APIToKinesisTimeMs',
                        'Dimensions': [
                            {
                                'Name': 'Env',
                                'Value': project_env
                            },
                            {
                                'Name': 'Entity',
                                'Value': entity
                            },
                        ],
                        'Unit': 'Milliseconds',
                        'Value': api_to_kinesis_time_ms
                    },
                    {
                        'MetricName': 'KinesisToDynamoTimeMs',
                        'Dimensions': [
                            {
                                'Name': 'Env',
                                'Value': project_env
                            },
                            {
                                'Name': 'Entity',
                                'Value': entity
                            },
                        ],
                        'Unit': 'Milliseconds',
                        'Value': kinesis_to_dynamo_time_ms
                    },
                    {
                        'MetricName': 'StoreToDynamoLambdaTime',
                        'Dimensions': [
                            {
                                'Name': 'Env',
                                'Value': project_env
                            },
                            {
                                'Name': 'Entity',
                                'Value': entity
                            },
                        ],
                        'Unit': 'Milliseconds',
                        'Value': lambda_to_dynamo_time_ms
                    },
                ],
                Namespace='UciTcl/dataPipeline'
            )
            logger.debug("Sent metrics to cloudwatch: TotalPipelineTimeMs: %s ; ApiToKinesisTimeMs: %s ; KinesisToDynamoTimeMs %s" % (
                total_pipeline_time_ms, api_to_kinesis_time_ms, kinesis_to_dynamo_time_ms))
        except Exception as ex:
            logger.error("Failed to send metrics to cloudwatch:")
            logger.error(ex, exc_info=True)

def trigger_personal_best(table_name_live, table_name_static, race_details):

    response_race_information = dynamodb_client.query(
        ScanIndexForward=False,
        TableName=table_name_static,
        KeyConditionExpression='pk = :myPK AND sk = :mySK',
        ExpressionAttributeValues={
            ':myPK': {'S': 'EVENT#EventID=' + race_details["RaceID"][:6] + '#'},
            ':mySK': {'S': 'RACE#RaceID=' + race_details["RaceID"] + '#'}
        }
    )
    race_information = response_race_information["Items"][0]

    subject = SUBJECT + race_information["RaceName"]["S"]
    body_text = BODY_TEXT + "Race ID: " + race_details["RaceID"] + "\r\n\r\n"
    logger.debug("Computing personal bests for RaceID: %s", race_details["RaceID"])

    response_agg_personalbest = dynamodb_client.query(
        IndexName='pk-EventTimeStamp-index',
        ScanIndexForward=False,
        TableName=table_name_live,
        KeyConditionExpression='pk = :myPK',
        ExpressionAttributeValues={
            ':myPK': {'S': 'AGG#PERSONALBEST#RaceID=' + race_details["RaceID"] + '#'}
        }
    )
    logger.debug("in trigger_personal_best response for AGG#PERSONALBEST: " + str(response_agg_personalbest["Items"]))

    UCIID_list = {}
    for result in response_agg_personalbest["Items"]:
        if result["UCIID"]["S"] not in UCIID_list:
            UCIID_list[result["UCIID"]["S"]] = [result]
        else:
            UCIID_list[result["UCIID"]["S"]].append(result)
    logger.debug("Computed UCIID list of personal best aggregates: %s", UCIID_list)

    for UCIID in UCIID_list:
        logger.debug("UCIID %s", UCIID)
        body_text = body_text + "UCIID: " + UCIID + "\r\n"
        response_preseason_questionnaire = dynamodb_client.query(
            ScanIndexForward=False,
            TableName=table_name_static,
            KeyConditionExpression='pk = :myPK AND sk = :mySK',
            ExpressionAttributeValues={
                ':myPK': {'S': 'RIDER#UCIID=' + UCIID + '#'},
                ':mySK': {'S': 'RIDER#'}
            }
        )
        preseason_questionnaire = response_preseason_questionnaire["Items"][0]
        logger.debug("Pre Season Questionnaire details for UCIID: " + UCIID + " are " + str(preseason_questionnaire))

        # Finding the personal best value and associated timestamp
        max_fields = {
            "UCIID": UCIID,
            "max_RiderHeartrate": [0, ""],
            "max_RiderPower": [0, ""],
            "max_Power5s": [0, ""],
            "max_Power15s": [0, ""],
            "max_Power30s": [0, ""],
            "max_Power60s": [0, ""],
            "max_Power120s": [0, ""],
            "max_Power180s": [0, ""],
            "max_Power300s": [0, ""],
            "max_Power600s": [0, ""]
        }

        # Iterating over personal best aggregates for each rider and finding maximum where PSQ value was exceeded
        for aggregate in UCIID_list[UCIID]:
            if int(aggregate["RiderHeartrateExceeded"]["N"]) == 1:
                if int(aggregate["RiderHeartrate"]["N"]) > max_fields["max_RiderHeartrate"][0]:
                    max_fields["max_RiderHeartrate"][0] = int(aggregate["RiderHeartrate"]["N"])
                    max_fields["max_RiderHeartrate"][1] = str(aggregate["EventTimeStamp"]["S"])
            if int(aggregate["RiderPowerExceeded"]["N"]) == 1:
                if int(aggregate["RiderPower"]["N"]) > max_fields["max_RiderPower"][0]:
                    max_fields["max_RiderPower"][0] = int(aggregate["RiderPower"]["N"])
                    max_fields["max_RiderPower"][1] = str(aggregate["EventTimeStamp"]["S"])
            for field in POWER_FIELDS:
                if int(aggregate[field + "Exceeded"]["N"]) == 1:
                    if int(aggregate[field]["N"]) > max_fields["max_" + field][0]:
                        max_fields["max_" + field][0] = int(aggregate[field]["N"])
                        max_fields["max_" + field][1] = str(aggregate["EventTimeStamp"]["S"])
        logger.debug("Rider maximums for RaceID: " + race_details["RaceID"] + " when personal best exceeded: " + str(max_fields))

        body_text = construct_email_body_personal_best(max_fields, preseason_questionnaire, body_text)

    send_email_personal_best(subject, body_text, race_details["RaceID"])

def percentage(previous, current):
    Percentage = round(100 * float(current - previous) / float(previous), 3)
    return str(Percentage) + "%"

def construct_email_body_personal_best(max_fields, preseason_questionnaire, body_text):

    if max_fields["max_RiderHeartrate"][0] > int(preseason_questionnaire["MaxHrBpm"]["S"]):
        body_text = body_text + "Event Timestamp: " + str(max_fields["max_RiderHeartrate"][1]) + "\r\n"
        body_text = body_text + "Previous maxiumum rider heartrate: " + preseason_questionnaire["MaxHrBpm"]["S"] + "\r\n"
        body_text = body_text + "New maximum rider heartrate: " + str(max_fields["max_RiderHeartrate"][0]) + "\r\n"
        body_text = body_text + "Delta percentage: " + percentage(int(preseason_questionnaire["MaxHrBpm"]["S"]),
                                                                  max_fields["max_RiderHeartrate"][0]) + "\r\n\r\n"
    if max_fields["max_RiderPower"][0] > int(preseason_questionnaire["PowerPeakW"]["S"]):
        body_text = body_text + "Event Timestamp: " + str(max_fields["max_RiderPower"][1]) + "\r\n"
        body_text = body_text + "Previous peak rider power: " + preseason_questionnaire["PowerPeakW"]["S"] + "\r\n"
        body_text = body_text + "New peak rider power: " + str(max_fields["max_RiderPower"][0]) + "\r\n"
        body_text = body_text + "Delta percentage: " + percentage(int(preseason_questionnaire["PowerPeakW"]["S"]),
                                                                  max_fields["max_RiderPower"][0]) + "\r\n\r\n"
    for field in POWER_FIELDS:
        if max_fields["max_" + field][0] > int(preseason_questionnaire[field + "W"]["S"]):
            body_text = body_text + "Event Timestamp: " + str(max_fields["max_" + field][1]) + "\r\n"
            body_text = body_text + "Previous peak rider " + field + "W: " + preseason_questionnaire[field + "W"]["S"] + "\r\n"
            body_text = body_text + "New peak rider " + field + "W: " + str(max_fields["max_" + field][0]) + "\r\n"
            body_text = body_text + "Delta percentage: " + percentage(int(preseason_questionnaire[field + "W"]["S"]),
                                                                      max_fields["max_" + field][0]) + "\r\n\r\n"
    return body_text

def send_email_personal_best(subject, body_text, race_id):

    try:
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
                'CcAddresses': [ # REMOVE FOR PROD
                    CC,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': subject,
                },
            },
            Source=SENDER,
        )
    except Exception as ex:
        logger.info("Failed to send personal best email notification")
        logger.error(ex, exc_info=True)
    else:
        logger.info("Personal best email for RaceID: %s sent. Response MessageID %s: ", race_id, response['MessageId'])
