import json
import boto3
import os
import requests
from requests.auth import HTTPBasicAuth

ES_ENDPOINT  = os.environ['ES_ENDPOINT']
ES_USER      = os.environ['ES_USER']
ES_PASS      = os.environ['ES_PASS']
REGION       = os.environ['REGION']
BOT_ID       = os.environ['BOT_ID']
BOT_ALIAS_ID = os.environ['BOT_ALIAS_ID']
ES_INDEX     = 'photos'
LOCALE_ID    = 'en_US'

lex = boto3.client('lexv2-runtime', region_name=REGION)

def get_keywords_from_lex(query):
    response = lex.recognize_text(
        botId=BOT_ID,
        botAliasId=BOT_ALIAS_ID,
        localeId=LOCALE_ID,
        sessionId='search-session-001',
        text=query
    )
    
    print(f"Lex full response: {response}")
    
    keywords = []
    interpretations = response.get('interpretations', [])
    
    for interp in interpretations:
        intent = interp.get('intent', {})
        if intent.get('name') == 'SearchIntent':
            slots = intent.get('slots', {})
            for slot_name, slot_val in slots.items():
                if slot_val and slot_val.get('value'):
                    val = slot_val['value'].get('interpretedValue', '').strip().lower()
                    if val:
                        keywords.append(val)
    
    print(f"Extracted keywords: {keywords}")
    return keywords

def search_opensearch(keywords):
    should_clauses = [{"term": {"labels": kw}} for kw in keywords]
    
    query = {
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
    }
    
    url = f"{ES_ENDPOINT}/{ES_INDEX}/_search"
    print(f"Querying OpenSearch: {url} with {query}")
    
    response = requests.get(
        url,
        auth=HTTPBasicAuth(ES_USER, ES_PASS),
        headers={'Content-Type': 'application/json'},
        json=query,
        verify=True
    )
    
    print(f"OpenSearch response: {response.status_code} — {response.text}")
    
    hits = response.json().get('hits', {}).get('hits', [])
    
    photos = []
    for hit in hits:
        src = hit['_source']
        photos.append({
            'url': f"https://{src['bucket']}.s3.amazonaws.com/{src['objectKey']}",
            'labels': src['labels']
        })
    
    return photos

def cors_headers():
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,x-api-key,x-amz-meta-customlabels',
        'Access-Control-Allow-Methods': 'GET,PUT,OPTIONS'
    }

def lambda_handler(event, context):
    print(f"Incoming event: {event}")
    
    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers(),
            'body': ''
        }
    
    query = ''
    if event.get('queryStringParameters'):
        query = event['queryStringParameters'].get('q', '').strip()
    
    print(f"Search query: {query}")
    
    if not query:
        return {
            'statusCode': 200,
            'headers': cors_headers(),
            'body': json.dumps({'results': []})
        }
    
    keywords = get_keywords_from_lex(query)
    
    if not keywords:
        print("No keywords extracted from Lex")
        return {
            'statusCode': 200,
            'headers': cors_headers(),
            'body': json.dumps({'results': []})
        }
    
    photos = search_opensearch(keywords)
    
    return {
        'statusCode': 200,
        'headers': cors_headers(),
        'body': json.dumps({'results': photos})
    }