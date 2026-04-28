import json
import boto3
import datetime
import urllib.parse
import os
import requests
from requests.auth import HTTPBasicAuth

# Pull config from environment variables (you set these in Configuration tab)
ES_ENDPOINT = os.environ['ES_ENDPOINT']
ES_USER     = os.environ['ES_USER']
ES_PASS     = os.environ['ES_PASS']
REGION      = os.environ['REGION']
ES_INDEX    = 'photos'

rekognition = boto3.client('rekognition', region_name=REGION)
s3          = boto3.client('s3',          region_name=REGION)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        print(f"Processing: s3://{bucket}/{key}")
        
        # Step 1: Detect labels using Rekognition
        rek_response = rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            MaxLabels=10,
            MinConfidence=70
        )
        labels = [label['Name'].lower() for label in rek_response['Labels']]
        print(f"Rekognition labels: {labels}")
        
        # Step 2: Get custom labels from S3 object metadata
        head = s3.head_object(Bucket=bucket, Key=key)
        metadata = head.get('Metadata', {})
        
        # Note: AWS lowercases all metadata keys, so x-amz-meta-customLabels becomes 'customlabels'
        custom_raw = metadata.get('customlabels', '')
        if custom_raw:
            custom_labels = [l.strip().lower() for l in custom_raw.split(',')]
            print(f"Custom labels: {custom_labels}")
            labels.extend(custom_labels)
        
        # Step 3: Build the document to store in OpenSearch
        doc = {
            'objectKey':        key,
            'bucket':           bucket,
            'createdTimestamp': datetime.datetime.now().isoformat(),
            'labels':           labels
        }
        print(f"Document to index: {doc}")
        
        # Step 4: POST the document to OpenSearch
        url = f"{ES_ENDPOINT}/{ES_INDEX}/_doc"
        response = requests.post(
            url,
            auth=HTTPBasicAuth(ES_USER, ES_PASS),
            headers={'Content-Type': 'application/json'},
            json=doc,
            verify=True
        )
        print(f"OpenSearch response: {response.status_code} — {response.text}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Indexing complete')
    }