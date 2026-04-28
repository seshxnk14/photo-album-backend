import json
import boto3
import datetime
import urllib.parse
import os
import requests
from requests.auth import HTTPBasicAuth

ES_ENDPOINT = os.environ['ES_ENDPOINT']
ES_USER     = os.environ['ES_USER']
ES_PASS     = os.environ['ES_PASS']
REGION      = os.environ['REGION']
ES_INDEX    = 'photos'

rekognition = boto3.client('rekognition', region_name=REGION)
s3          = boto3.client('s3', region_name=REGION)

def get_content_type(key):
    key_lower = key.lower()
    if key_lower.endswith('.jpg') or key_lower.endswith('.jpeg'):
        return 'image/jpeg'
    elif key_lower.endswith('.png'):
        return 'image/png'
    elif key_lower.endswith('.gif'):
        return 'image/gif'
    elif key_lower.endswith('.webp'):
        return 'image/webp'
    else:
        return 'image/jpeg' 

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = urllib.parse.unquote_plus(record['s3']['object']['key'])

        print(f"Processing: s3://{bucket}/{key}")

        try:
            correct_content_type = get_content_type(key)
            print(f"Setting Content-Type to: {correct_content_type}")

            s3.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource={'Bucket': bucket, 'Key': key},
                ContentType=correct_content_type,
                MetadataDirective='REPLACE'
            )
            print(f"Content-Type fixed to {correct_content_type}")

            s3_object = s3.get_object(Bucket=bucket, Key=key)
            image_bytes = s3_object['Body'].read()
            print(f"Downloaded {len(image_bytes)} bytes")

            rek_response = rekognition.detect_labels(
                Image={'Bytes': image_bytes},
                MaxLabels=10,
                MinConfidence=70
            )
            labels = [label['Name'].lower() for label in rek_response['Labels']]
            print(f"Rekognition labels: {labels}")

        except Exception as e:
            print(f"Rekognition error: {e}")
            labels = []

        # Get custom labels from S3 metadata
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            metadata = head.get('Metadata', {})
            custom_raw = metadata.get('customlabels', '')
            if custom_raw:
                custom_labels = [l.strip().lower() for l in custom_raw.split(',')]
                print(f"Custom labels: {custom_labels}")
                labels.extend(custom_labels)
        except Exception as e:
            print(f"Metadata error: {e}")

        doc = {
            'objectKey':        key,
            'bucket':           bucket,
            'createdTimestamp': datetime.datetime.now().isoformat(),
            'labels':           labels
        }
        print(f"Document to index: {doc}")

        try:
            url = f"{ES_ENDPOINT}/{ES_INDEX}/_doc"
            response = requests.post(
                url,
                auth=HTTPBasicAuth(ES_USER, ES_PASS),
                headers={'Content-Type': 'application/json'},
                json=doc,
                verify=True
            )
            print(f"OpenSearch response: {response.status_code} — {response.text}")
        except Exception as e:
            print(f"OpenSearch error: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Indexing complete')
    }