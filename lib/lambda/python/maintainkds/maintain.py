import boto3
import logging
import os
import time

# Collect info from ENV values and set variables
if os.environ['ENABLED'] == 'true':
    ENABLED = True
else:
    ENABLED = False
if os.environ['RESTART'] == 'true':
    RESTART = True
else:
    RESTART = False

RESTARTTIME = os.environ['RESTARTTIME']
APP = os.environ['APP']

# Enable logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Functions
def StartKDS(client, APP):
    # Get additional info about app
    appInfo = client.describe_application(
        ApplicationName=APP
    )
    if appInfo['ApplicationDetail']['ApplicationStatus'] == 'READY':
        # Start the app
        logger.info('Start the app')
        response = client.start_application(
            ApplicationName=APP,
            InputConfigurations=[
                {
                    'Id': (appInfo['ApplicationDetail']['InputDescriptions'][0]['InputId']),
                    'InputStartingPositionConfiguration': {
                        'InputStartingPosition': 'NOW'
                    }
                },
            ]
        )
        logger.info(response)
    else:
        if appInfo['ApplicationDetail']['ApplicationStatus'] == 'RUNNING':
            logger.info('The App is in RUNNING state. Nothing to do. Exit.')
        else:
            logger.info('The App is in the wrong state. Exit.')
            return 0
    return 1

def RestartKDS(client, APP):
    # Get additional info about app
    appInfo = client.describe_application(ApplicationName=APP)

    # Stop the app
    logger.info('Stop the app')
    if appInfo['ApplicationDetail']['ApplicationStatus'] == 'RUNNING':
        response = client.stop_application(ApplicationName=APP)
        logger.info(response)
    else:
        logger.info('The app is not in Running state. Can not stop it')

    # Start the app. Check again that you are in the ready state.
    if appInfo['ApplicationDetail']['ApplicationStatus'] == 'READY':
        StartKDS(client, APP)
    else:
        # Waiting for restart for 2 minutes (12*10)/60 = 2min
        for i in range(12):
            logger.info(f'{i} sleep for 10 seconds')
            time.sleep(10)
            appInfo = client.describe_application(ApplicationName=APP)
            if appInfo['ApplicationDetail']['ApplicationStatus'] == 'READY':
                break
        # If timeout
        if i == 11:
            logger.info('The App is not in the ready state. Can not start it. Exit.')
            return 0
        # Not a timeout (i<11), than start the app
        StartKDS(client, APP)
    # Exit
    return 1


# Execution
def lambda_handler(event, context):
    if ENABLED:
        logger.info(event)
        client = boto3.client('kinesisanalytics')
        if RESTART and RESTARTTIME in event:
            # Check the time and restart app if match
            RestartKDS(client, APP)
            # exit from the function
            return 1
        # Regular execution
        StartKDS(client, APP)
        return 1
    else:
        logger.info('The ENABLED flag is not true. Exit')
        return 0
