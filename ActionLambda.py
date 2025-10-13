import boto3   # for interacting with different AWS services
import time
import os
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime

ATHENA_OUTPUT_BUCKET = "s3://bkt-intellekt-athena-output/"  # S3 bucket where Athena will put the results
DATABASE = 'intellekt'  # The name of the database in Athena


def lambda_handler(event, context):
    
    result = ''
    roomDetails=''
    response_code = 200
    action_group = event['actionGroup']
    api_path = event['apiPath']
    
    def get_named_parameter(event, name):
        return next(item for item in event['parameters'] if item['name'] == name)['value']     

    def getRoomDetails(name):
        roomName = get_named_parameter(event, 'name').lower()
        print("NAME PRINTED: ", roomName)
        
        client = boto3.client('athena')   # create anthena client
        QUERY = f"SELECT * FROM bkt_intellekt_roomdevices where OfficeName='{roomName}'"
        print(QUERY)
        
        # Start the Athena query execution
        response = client.start_query_execution(
            QueryString=QUERY,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': ATHENA_OUTPUT_BUCKET
            }
        )
        #print(response)

        query_execution_id = response['QueryExecutionId']
    
        while True:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:  # (optional) checking the status 
                break
            
            time.sleep(5)  # Poll every 5 seconds
        
        # Here, you can handle the response as per your requirement
        if state == 'SUCCEEDED':
            # Fetch the results if necessary
            print('succeeded')
            result_data = client.get_query_results(QueryExecutionId=query_execution_id)
            
            header_row =  result_data['ResultSet']['Rows'][0]['Data']
            value_row = result_data['ResultSet']['Rows'][1]['Data']
            
            roomDetails = {"officename": value_row[0]['VarCharValue'], 
            "fancoilunit":  value_row[1]['VarCharValue'], 
            "iotdevice":  value_row[2]['VarCharValue']}
            
            print('header')
            print(roomDetails)
            return roomDetails
            # Print the results as Header | Value
            #for header, value in zip([h['VarCharValue'] for h in header_row], [v['VarCharValue'] for v in value_row]):
                #print(f"{header} | {value}")
        else:
            print(state)
            return state
       
    def getRoomMetrics(asset_id):

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('intellekt_iot_data')

        # Get the asset_id from the event
        #asset_id='0004a30b0127303f'
        asset_id = get_named_parameter(event, 'assetId').lower()
        print("asset_id PRINTED: ", asset_id)
        #asset_id = event.get('asset_id')
        #print(asset_id)

        if not asset_id:
            return {
                'statusCode': 400,
                'body': 'asset_id parameter is required'
            }

        try:
            # Query the table for items with the given asset_id
            response = table.query(
                KeyConditionExpression=Key('asset_id').eq(asset_id),
                ScanIndexForward=False,  # Sort in descending order
                Limit=1  # Get only the latest record
            )

            items = response.get('Items', [])

            if not items:
                return {
                    'statusCode': 404,
                    'body': f'No records found for asset_id: {asset_id}'
                }

            # Return the latest record
            latest_record = items[0]
            print(latest_record)
            return latest_record

        except Exception as e:
            print(f"Error: {str(e)}")
            return {
                'statusCode': 500,
                'body': f'An error occurred: {str(e)}'
            }    
            
    print("api_path: ", api_path )
    
    if api_path == '/roomDetails':
        result = getRoomDetails(event)
    elif api_path == '/roomMetrics':
        result = getRoomMetrics(event)
    else:
        response_code = 404
        result = f"Unrecognized api path: {action_group}::{api_path}"
    
    response_body = {
    'application/json': {
        'body': result
        }
    }
    
    action_response = {
    'actionGroup': event['actionGroup'],
    'apiPath': event['apiPath'],
    'httpMethod': event['httpMethod'],
    'httpStatusCode': response_code,
    'responseBody': response_body
    }

    api_response = {'messageVersion': '1.0', 'response': action_response}
    print(api_response)
    return api_response
