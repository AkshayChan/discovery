"""
 This function is used to generate artificial race data including:
    race_list, race_start_list, live telemetry data, and race results.
 The main purpose is to generate flow of data, so all telemertry values are hardcoded.

 Usage: Invoke, by hitting Test with any event. Check logs for generation output
"""
import os
import json
import copy
from time import sleep
import datetime
import logging
import urllib3
from random import random, randint
import random as rand
import boto3
from botocore.exceptions import ClientError

GenderType = rand.choice(['Women', 'Man'])

RaceState = rand.choice(['Provisional', 'Official'])

RaceType = rand.choice(['Sprint', 'Keirin','Elimination','Scratch', 'League'])

RaceLeague = rand.choice(['Men Endurance', 'Women Endurance'])

if RaceType == "Sprint" or  RaceType == "Keirin":
    RaceLeague = rand.choice(['Men Sprint', 'Women Sprint'])

eventId = rand.randrange(202101, 202106)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
https = urllib3.PoolManager()


def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    logger.info('Decrypting API key from secrets manager called: {}'.format(secret_name))
    return json.loads(get_secret_value_response['SecretString'])['api_key']


ENDPOINT_URL_RIDERSTRACKING = os.environ['API_URL'] + "StoreLiveRidersTracking"
ENDPOINT_URL_RIDERSDATA = os.environ['API_URL'] + "StoreLiveRidersData"
ENDPOINT_URL_QUESTIONNAIRELIST = os.environ['API_URL'] + "StorePreSeasonQuestionnaire"
ENDPOINT_URL_RACELIST = os.environ['API_URL'] + "StoreRacesList"
ENDPOINT_URL_RACESTARTLIST = os.environ['API_URL'] + "StoreRaceStartList"
ENDPOINT_URL_RACERESULTS = os.environ['API_URL'] + "StoreRaceResults"
ENDPOINT_URL_LAPCOUNTER = os.environ['API_URL'] + "StoreLapCounter"
ENDPOINT_URL_FINISHTIME = os.environ['API_URL'] + "StoreFinishTime"
ENDPOINT_URL_STARTTIME = os.environ['API_URL'] + "StoreStartTime"
ENDPOINT_URL_RIDERELIMINATED = os.environ['API_URL'] + "StoreRiderEliminated"
ENDPOINT_URL_CLASSIFICATION = os.environ['API_URL'] + "StoreClassification"
API_KEY = get_secret(os.environ['SECRET_MANAGER_NAME_FOR_API_KEY'], os.environ['AWS_REGION'])


def lambda_handler(event, context):
    # Config variables
    RACES_COUNT = int(os.environ['RACES_COUNT'])  # number of races in competition
    RACER_COUNT = int(os.environ['RACER_COUNT'])  # number of riders in each race
    RACE_DURATION_SEC = int(os.environ['RACE_DURATION_SEC'])  # race duration in seconds

    if ("queryStringParameters" in event):
        queryStringParameters = event["queryStringParameters"]
        if ("race_duration_sec" in queryStringParameters):
            RACE_DURATION_SEC = int(queryStringParameters['race_duration_sec'])
        if ("racer_count" in queryStringParameters):
            RACER_COUNT = int(queryStringParameters['racer_count'])
        if ("races_count" in queryStringParameters):
            RACES_COUNT = int(queryStringParameters['races_count'])

    # Filling Questionnaire
    questionnaire_list = generate_Questionnaire(RACER_COUNT)
    call_api_gateway(questionnaire_list, ENDPOINT_URL_QUESTIONNAIRELIST, API_KEY)

    # Generate the race list
    race_list = generate_RaceList(RACES_COUNT)
    call_api_gateway(race_list, ENDPOINT_URL_RACELIST, API_KEY)
    logger.info('race_list = {}'.format(str(race_list)))

    # for every race generate startlist, realtime feed and race_results
    for race in race_list['Races']:
        logger.info('Running test race {} '.format(str(race['RaceID'])))

        # generate RaceStartList
        race_start_list = generate_RaceStartList(RACER_COUNT, race['RaceID'], race['RaceName'])
        call_api_gateway(race_start_list, ENDPOINT_URL_RACESTARTLIST, API_KEY)
        logger.info('race_start_list = {}'.format(str(race_start_list)))

        # generate StartTime
        start_time = generate_StartTime(race_start_list['RaceID'])
        call_api_gateway(start_time, ENDPOINT_URL_STARTTIME, API_KEY)
        logger.info('start_time = {}'.format(str(start_time)))

        # generate RaceStartLive
        race_start_live = generate_RaceStartLive(race_start_list['RaceID'])
        call_api_gateway(race_start_live, ENDPOINT_URL_STARTTIME, API_KEY)
        logger.info('race_start_live = {}'.format(str(race_start_live)))

        # generate real time metrics LiveRidersTracking and LiveRidersData
        logger.info('Started generating realtime events')
        generate_real_time_race(race_start_list, race['RaceID'], RACE_DURATION_SEC)
        logger.info('Finished generating realtime events')

        # generate FinishTime
        finish_time = generate_FinishTime(race_start_list['RaceID'])
        call_api_gateway(finish_time, ENDPOINT_URL_FINISHTIME, API_KEY)
        logger.info('finish_time = {}'.format(str(finish_time)))

        # generate RaceResults
        race_results = generate_RaceResults(race_start_list, race['RaceID'], race['RaceName'])
        call_api_gateway(race_results, ENDPOINT_URL_RACERESULTS, API_KEY)
        logger.info("race_results = {}".format(str(race_results)))

        # generate classifications
        classification = generate_Classification(race_start_list)
        logger.info("classification = {}".format(str(classification)))
        call_api_gateway(classification, ENDPOINT_URL_CLASSIFICATION, API_KEY)

        logger.info('End of race {} '.format(str(race['RaceID'])))

    return {
        'statusCode': 200,
        'body': json.dumps('Race simulation done!')
    }


def get_real_life_uciids():
    """
    Method has hardcoded UCIIDs from https://www.uci.org/track/rankings so that we can generate correct IDs
    This method might get rider details from uci.org directly

    :return: List of UCIIDs
    :rtype: list
    """
    return [
        10096523064, 10118842057, 10036069533, 10119244609, 10096554083, 10116102920, 10050725021, 10015145623,
        10054290981, 10022147003, 10020998157, 10084556395, 10085039476, 10054613206, 10073343195, 10017653778,
        10004622638, 10064652605, 10097389495, 10113586879, 10010794262, 10022183981, 10005305577, 10005227472,
        10015914246, 10118906422, 10116103425, 10007227995, 10088306861, 10011164276, 10084652991, 10118877019,
        10009333404, 10009030377, 10088306962, 10021645431, 10012448518, 10015992654, 10118877322, 10064647753,
        10116103324, 10075185084, 10022102846, 10109371019, 10001290585, 10016270116, 10021158007, 10096418485,
        10081576475, 10021785776, 10096367763, 10008958033, 10020925712, 10082239917, 10116103223, 10021316439,
        10005305274, 10006065817, 10066468727, 10077024448, 10021022207, 10004725092, 10109680914, 10016327306,
        10119068793, 10020805167, 10015239387, 10095970366, 10086865706, 10013667078, 10036021740, 10073282672,
        10021553784, 10015914145, 10119083547, 10109370817, 10011074249, 10036022649, 10006422895, 10009972994,
        10063243475, 10015171992, 10036242820, 10048325683, 10048324774, 10059218884, 10062941765, 10088731237,
        10022807613, 10023156510, 10023189953, 10007271849, 10009328047, 10015267477, 10034928165, 10034934431,
        10051132421, 10054592186, 10063232563, 10082410675, 10089917162, 10103577792, 10010726261, 10020974616
    ]


def call_api_gateway(payload, url, api_key):
    """
    TBD after defining api gateway
    :param dict payload: message to send
    :param str url: URL to api endpoint
    :param str api_key: Api key used for access
    :return: the request response
    :rtype: int
    """
    headers = {"Content-Type": "application-json", "X-Api-Key": api_key}

    response = https.request(
        "POST",
        url,
        body=json.dumps(payload),
        headers=headers
    )

    logger.info('API responded with = {}; body={}'.format(response.status, response.data))

    return response


def time_now_str(simplified=False):
    """
    Generates start date like 2021-08-11T09:52:42.244095Z
    :param str simplified:
    :return: actual timestamp
    :rtype: str
    """
    return datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S.%f')[:-3] if simplified \
        else datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ')


def generate_RaceList(races_count):
    """
    Generates RaceList based on defined number
    :param int races_count: how many races are in this competition
    :return: Json with array of races
    :rtype: dict
    """
    RacesList = copy.deepcopy(RacesList_template)
    for i in range(races_count):
        race = copy.deepcopy(RacesList_race_template)
        race['RaceID'] = int(str(RacesList['EventID']) + "0" + str(i + 7))
        race['RaceName'] = 'RACE test {}'.format(i + 1)
        RacesList['Races'].append(race)
    return RacesList


def generate_RaceStartList(racers_count, race_id, race_name):
    """
    Generates genrate_RaceStartList based on defined number
    :param int racers_count: how many racers are in this race
    :param int race_id: id of race
    :param int race_name: name of race
    :return: Json with array of racers who take part in single race
    :rtype: dict
    """
    RaceStartlist = copy.deepcopy(RaceStartlist_template)
    RaceStartlist['RaceID'] = race_id
    RaceStartlist['RaceName'] = race_name
    for i in range(racers_count):
        person = copy.deepcopy(RaceStartlistRider_template)
        person['Bib'] = i + 1
        person['UCIID'] = uciids[i]
        person['FirstName'] = 'TEST FirstName {}'.format(i + 1)
        person['LastName'] = 'TEST LastName {}'.format(i + 1)
        RaceStartlist['Startlist'].append(person)
    return RaceStartlist


def generate_StartTime(race_id):
    """
    Generates StartTime for particular race
    :param int race_id: id of race
    :return: Json with StartTime object
    :rtype: dict
    """
    StartTime = copy.deepcopy(StartTime_template)
    StartTime['TimeStamp'] = time_now_str()
    StartTime['RaceID'] = race_id
    return StartTime

def generate_RaceStartLive(race_id):
    """
    Generates RaceStartLive for particular race
    :param int race_id: id of Race
    :return: Json with RaceStartLive object
    :rtype: dict
    """
    RaceStartLive = copy.deepcopy(RaceStartLive_template)
    RaceStartLive['TimeStamp'] = time_now_str()
    RaceStartLive['RaceID'] = race_id
    return RaceStartLive

def generate_FinishTime(race_id):
    """
    Generates FinishTime for particular race
    :param int race_id: list of participants in this race
    :return: Json with StartTime object
    :rtype: dict
    """
    FinishTime = copy.deepcopy(FinishTime_template)
    FinishTime['TimeStamp'] = time_now_str()
    FinishTime['RaceID'] = race_id
    return FinishTime


def generate_real_time_race(race_start_list, race_id, duration):
    """
    Generates LiveRidersTracking, LiveRidersData, LapCounter on the fly.
    It simulates race with similar data frequency which was defined in documentation.
    Method stores this data in kinesis stream
    :param dict race_start_list: how many racers are in this race
    :param int race_id: id of race
    :param int duration: duration of race in seconds
    :return: None (just processing)
    """
    n = 0
    while n < duration * 10:  # 10 sec of race
        LiveRidersTracking = copy.deepcopy(LiveRidersTracking_template)
        LiveRidersTracking['TimeStamp'] = time_now_str()
        LiveRidersTracking['RaceID'] = race_id

        LiveRidersData = copy.deepcopy(LiveRidersData_template)

        # logger.info(str(n) +' iter 10HZ: ' + time_now_str())
        for racer in race_start_list['Startlist']:
            LiveRidersTrackingCapture = copy.deepcopy(LiveRidersTrackingCaptures_template)
            LiveRidersTrackingCapture['Bib'] = str(racer['Bib'])
            LiveRidersTrackingCapture['UCIID'] = racer['UCIID']
            LiveRidersTrackingCapture['Speed'] = round(20 + n / 10 + random(), 2)

            LiveRidersTrackingCapture['TimeStamp'] = time_now_str(simplified=True)
            LiveRidersTracking['Captures'].append(LiveRidersTrackingCapture)
            if n % 10 == 0:
                LiveRidersData['TimeStamp'] = time_now_str()
                LiveRidersData['RaceID'] = race_id
                LiveRidersDataCapture = copy.deepcopy(LiveRidersDataCaptures_template)
                LiveRidersDataCapture['Bib'] = str(racer['Bib'])
                LiveRidersDataCapture['UCIID'] = racer['UCIID']
                #LiveRidersDataCapture['Heartrate'] = int(60 + n / 10 + randint(1, 9))
                LiveRidersDataCapture['Heartrate'] = None
                LiveRidersDataCapture['Cadency'] = int(100 + n / 10 + randint(1, 12))
                LiveRidersDataCapture['Power'] = int(120 + n / 10 + randint(10, 25))
                LiveRidersDataCapture['TimeStamp'] = time_now_str(simplified=True)
                LiveRidersData['Captures'].append(LiveRidersDataCapture)

        # Sleep 100ms and send 10Hz data every single iteration of the loop
        sleep(0.1)  # sleep 100 ms
        logger.info(LiveRidersTracking)
        call_api_gateway(LiveRidersTracking, ENDPOINT_URL_RIDERSTRACKING, API_KEY)

        # Send 1Hz data every 10th iteration of the loop
        if n % 10 == 0:
            logger.info(str(n) + ' iter 1HZ: ' + time_now_str())
            logger.info(LiveRidersData)
            call_api_gateway(LiveRidersData, ENDPOINT_URL_RIDERSDATA, API_KEY)

        # Send LapCounter data every 1000 iteration (~10sec) of the loop
        if n % 1000 == 0:
            LapCounter = copy.deepcopy(LapCounter_template)
            LapCounter['RaceID'] = race_id
            LapCounter['TimeStamp'] = time_now_str()
            call_api_gateway(LapCounter, ENDPOINT_URL_LAPCOUNTER, API_KEY)
            logger.info('LapCounter = {}'.format(str(LapCounter)))

        # Send RiderEliminated message every 1500 iteration (~15sec) of the loop
        if n % 1500 == 0:
            RiderEliminated = copy.deepcopy(RiderEliminated_template)
            RiderEliminated['RaceID'] = race_id
            RiderEliminated['TimeStamp'] = time_now_str()
            call_api_gateway(RiderEliminated, ENDPOINT_URL_RIDERELIMINATED, API_KEY)
            logger.info('RiderEliminated = {}'.format(str(RiderEliminated)))

        n = n + 1


def generate_RaceResults(race_start_list, race_id, race_name):
    """
    Generates RaceResults for particural race, based on start list data
    :param dict race_start_list: list of participants in this race
    :param int race_id: id of race
    :param int race_name: name of race
    :return: Json with array of race results who took part in single race
    :rtype: dict
    """
    RaceResults = copy.deepcopy(RaceResults_template)
    RaceResults['TimeStamp'] = time_now_str()
    RaceResults['RaceID'] = race_id
    RaceResults['RaceName'] = race_name
    for racer in race_start_list['Startlist']:
        RaceResults_individual = copy.deepcopy(RaceResults_individual_template)
        RaceResults_individual['Bib'] = str(racer['Bib'])
        RaceResults_individual['Rank'] = str(racer['Bib'])
        RaceResults_individual['UCIID'] = racer['UCIID']
        RaceResults_individual['FirstName'] = racer['FirstName']
        RaceResults['Results'].append(RaceResults_individual)
    return RaceResults


def generate_Classification(race_start_list):
    """
    Generates RaceResults for particural race, based on start list data
    :param dict race_start_list: list of participants in this race
    :return: Json with array of race results who took part in single race
    :rtype: dict
    """
    Classification = copy.deepcopy(Classification_template)
    Classification['TimeStamp'] = time_now_str()
    points = 40 # dummy data to have some points in place which will be subtracted by 2 for riders in the loop
    for racer in race_start_list['Startlist']:
        Classification_individual = copy.deepcopy(Classification_individual_template)
        Classification_individual['Bib'] = str(racer['Bib'])
        Classification_individual['Rank'] = str(racer['Bib'])
        Classification_individual['UCIID'] = racer['UCIID']
        Classification_individual['FirstName'] = racer['FirstName']
        Classification_individual['LastName'] = racer['LastName']
        Classification_individual['Points'] = points
        Classification['Results'].append(Classification_individual)
        points = points - 2
    return Classification


def generate_Questionnaire(racers_count):
    """
    Generates genrate_RaceStartList based on defined number
    :param int racers_count: how many racers are in this race
    :return: Json array of racers who filled questionnaire
    :rtype: dict
    """
    Questionnaire = []
    for i in range(racers_count):
        person = copy.deepcopy(Questionnaire_template)
        person['UCIID'] = uciids[i]
        person['FirstName'] = 'TEST name {}'.format(i + 1)
        Questionnaire.append(person)
    return Questionnaire


uciids = get_real_life_uciids()
####
# DATA SCHEMAS BASED ON DOCUMENTATION
####

# one time data RacesList ath the beginning
RacesList_template = {
    'Message': 'RunningOrder',
    'SeasonID': 2021,
    'EventID': eventId,
    'TimeStamp': '2021-11-05T18:30:00.391983Z',
    'Date': '2021-11-05T19:00:00.000000Z',
    'EventName': 'UCI TEST 2021 AWS',
    'Races': []
}

RacesList_race_template = {
    'RaceID': 20210201,
    'RaceType': RaceType,
    'Gender': 'Women',
    'League': RaceLeague,
    'Heat': 1,
    'TotalHeats': 6,
    'Round': 1,
    'TotalRounds': 6,
    'Laps': 40,
    'Distance': 10000,
    'RaceName': 'TEST Man Scratch Race',
    'StartTime': '2021-11-05T19:08:00.000000Z'
}

# one time data RaceStartlist
RaceStartlist_template = {
    'Message': 'StartList',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210201,
    'Gender': 'Women',
    'RaceType': RaceType,
    'League': RaceLeague,
    'Heat': 1,
    'TotalHeats': 6,
    'Round': 1,
    'TotalRounds': 6,
    'RaceName': 'TEST Man Scratch Race',
    'Laps': 40,
    'Distance': 10000,
    'TimeStamp': '2021-11-05T19:08:00.000000Z',
    'Startlist': []
}

RaceStartlistRider_template = {
    'Bib': '27',
    'UCIID': 27,
    'FirstName': 'TEST FirstName',
    'LastName': 'TEST LastName',
    'ShortTVName': 'F.LASTNAME',
    'Team': 'ITALY',
    'NOC': 'ITA',
    'Status': 'OK',
    'StartPosition': 1,
    'StartingLane': 0
}

# 10hz data LiveRidersTracking
LiveRidersTracking_template = {
    'Message': 'LiveRidersTracking',
    'TimeStamp': '2021-05-06T08:22:44.000000Z',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'Captures': []
}

LiveRidersTrackingCaptures_template = {
    'TimeStamp': '2021-05-06 08:22:44.000',
    'Bib': '10',
    'UCIID': 10035062450,
    'Rank': 1,
    'State': 'OK',
    'Distance': 426.8,
    'DistanceProj': 420.5,
    'Speed': 16.71,
    'SpeedMax': 17.65,
    'SpeedAvg': 16.55,
    'DistanceFirst': 0.0,
    'DistanceNext': 0.0,
    'Acc': 0.5803,
    'Pos': {
        'Lat': 48.85769407,
        'Lng': 2.20936353
    }
}

# 1Hz data from
LiveRidersData_template = {
    'Message': 'LiveRidersData',
    'TimeStamp': '2021-05-06 08:22:44.000',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'Captures': []
}

LiveRidersDataCaptures_template = {
    'TimeStamp': '2021-05-06 08:22:44.000',
    'Bib': '10',
    'UCIID': '10',
    'Heartrate': 148,
    'Cadency': 95,
    'Power': 282
}

# one-timer results of the race
RaceResults_template = {
    'Message': 'Results',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'RaceType': 'Scratch',
    'Gender': 'Women',
    'League': RaceLeague,
    'Heat': 1,
    'TotalHeats': 6,
    'Round': 1,
    'TotalRounds': 6,
    'State': RaceState,
    'RaceName': 'test Scratch Race',
    'Laps': 20,
    'Distance': 5000,
    'RaceTime': 612.510,
    'RaceSpeed': 55.0377,
    'TimeStamp': '2021-06-13T15:10:05.000000Z',
    'Results': []
}

RaceResults_individual_template = {
    'Rank': '1',
    'Bib': '27',
    'UCIID': '27',
    'FirstName': 'TEST FirstName',
    'LastName': 'TEST LastName',
    'ShortTVName': 'N.TEST',
    'Team': 'ITALY',
    'NOC': 'ITA',
    'Status': 'OK',
    'Laps': 50
}

StartTime_template = {
    'Message': 'StartTime',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'TimeStamp': '2021-07-29T03:58:50.391983Z'
}

RaceStartLive_template = {
    'Message': 'RaceStartLive',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'TimeStamp': '2021-07-29T03:58:50.391983Z'
}

FinishTime_template = {
    'Message': 'FinishTime',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'TimeStamp': '2021-07-29T03:58:50.391983Z',
    'RaceTime': '612.510',
    'RaceSpeed': 55.0377
}

LapCounter_template = {
    'Message': 'LapCounter',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210221,
    'LapsToGo': 20,
    'DistanceToGo': 5000,
    'TimeStamp': '2021-11-05T03:58:50.391983Z'
}

RiderEliminated_template = {
    'Message': 'RiderEliminated',
    'SeasonID': 2021,
    'EventID': eventId,
    'RaceID': 20210211,
    'RaceName': 'Elimination Men',
    'TimeStamp': '2021-07-28T23:21:15.391983Z',
    'Bib': '2',
    'UCIID': 10035062450,
    'FirstName': 'Valentin',
    'LastName': 'BUSH',
    'ShortTVName': 'V. BUSH',
    'Team': 'AUSTRIA',
    'NOC': 'AUT'
}

Questionnaire_template = {
    "FirstName": "John",
    "LastName": "Test",
    "BirthDate": "19/08/1900",
    "UCIID": "12345678901",
    "Gender": "Male",
    "TrainingLocation": "Velodrom Iles Baleares",
    "LeagueCat": "Endurance",
    "Nationality": "ESP",
    "SeasonTitle": "Sprint World Champion",
    "HeightCm": "175",
    "WeightKg": "70",
    "RestHrBpm": "42",
    "MaxHrBpm": "200",
    "Flying200": "00:10:25",
    "GearingForFlying200": "81,6",
    "PowerPeakW": "1350",
    "Power5sW": "1298",
    "Power15sW": "800",
    "Power30sW": "735",
    "Power60sW": "530",
    "Power120sW": "400",
    "Power180sW": "367",
    "Power300sW": "350",
    "Power600sW": "300",
    "Power1200sW": "290",
    "Power1800sW": "256",
    "Power3600sW": "243"
}

# one-timer results of the race
Classification_template = {
    'Message': 'Classification',
    'SeasonID': 2021,
    'EventID': eventId,
    'Gender': GenderType,
    'RaceType': RaceType,
    'League': RaceLeague,
    'State': RaceState,
    'TimeStamp': '2021-06-13T15:10:05.000000Z',
    'Results': []
}

Classification_individual_template = {
    'Rank': 1,
    'Bib': '27',
    'UCIID': 10035062450,
    'FirstName': 'TEST FirstName',
    'LastName': 'TEST LastName',
    'ShortTVName': 'N.TEST',
    'Team': 'ITALY',
    'NOC': 'ITA',
    'Status': 'OK',
    'Points': 40
}
