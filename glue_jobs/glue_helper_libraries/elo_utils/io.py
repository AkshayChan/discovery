import pandas as pd
import boto3

def put_elo_to_ddb(df: pd.DataFrame, ddb_table: str, session: boto3.Session):
    dynamodb_client = session.client('dynamodb')
    data_item = df.to_dict('index')
    for item in data_item:
        ddb_item = {
            'pk': {'S': "RIDER#UCIID=" + str(data_item[item].get('uciid', "")) + "#"},
            'sk': {'S': "FAVORITE#"},
            'UCIID': {'S': str(data_item[item].get('uciid', ""))},
            'EloMean': {'N': str(data_item[item].get('elo_mean', ""))},
            'EloStd': {'N': str(data_item[item].get('elo_std', ""))},
            'RaceType': {'S': str(data_item[item].get('racetype', ""))}
        }
        dynamodb_client.put_item(TableName=ddb_table, Item=ddb_item)  
    return 0