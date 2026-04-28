import json
import boto3
import os

REGION = os.environ['REGION']
BUCKET = os.environ['PHOTO_BUCKET']

s3 = boto3.client('s3', region_name=REGION)

def lambda_handler(event, context):
    print(f"Event: {event}")

    params = event.get('queryStringParameters') or {}
    filename     = params.get('filename', '')
    content_type = params.get('contentType', 'image/jpeg')
    custom_labels = params.get('customlabels', '')

    if not filename:
        return {
            'statusCode': 400,
            'headers': cors_headers(),
            'body': json.dumps({'error': 'filename is required'})
        }

    metadata = {}
    if custom_labels:
        metadata['customlabels'] = custom_labels

    presigned_url = s3.generate_presigned_url(
        'put_object',
        Params={
            'Bucket': BUCKET,
            'Key': filename,
            'ContentType': content_type,
            'Metadata': metadata
        },
        ExpiresIn=300
    )

    print(f"Generated presigned URL for {filename} with type {content_type}")

    return {
        'statusCode': 200,
        'headers': cors_headers(),
        'body': json.dumps({'uploadUrl': presigned_url})
    }

def cors_headers():
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
    }