import os
import json
import requests
import random
import string


def to_api(client_id, client_secret, dataframe, name, description, debug=False):
    api_endpoint = 'https://api.sofodata.com'
    tmp_file_location = '/tmp/' + get_random_alphanumeric_string(32) + '.csv'

    # Step 1 Save the dataframe to temp csv file
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
    if debug:
        print("Saving Dataframe to temp file " + tmp_file_location)
    dataframe.to_csv(path_or_buf=tmp_file_location, index=False)

    # Step 2 - Calculate the column headers
    # https://pbpython.com/pandas_dtypes.html
    column_headers = []
    for i in range(len(dataframe.columns)):
        column = dataframe.columns[i]
        if dataframe[column].dtype == 'bool':
            column_headers.append({
                "name": column,
                "type": "BOOLEAN",
                'indexed': i == 0
            })
        elif dataframe[column].dtype == 'int64':
            column_headers.append({
                "name": column,
                "type": "NUMBER",
                'indexed': i == 0
            })
        elif dataframe[column].dtype == 'float64':
            column_headers.append({
                "name": column,
                "type": "DECIMAL",
                'indexed': i == 0
            })
        else:
            column_headers.append({
                "name": column,
                "type": "STRING",
                'indexed': i == 0
            })

    # Step 3 - Create an OAuth access token
    if debug:
        print("Getting OAuth Access Token")
    url = api_endpoint + '/v8/oauth/token'
    headers = {'content-type': 'application/json'}
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "https://api.sofodata.com/",
        "grant_type": "client_credentials"
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    if debug:
        print(r.status_code)
        print("Response: " + r.text)
    access_token = json.loads(r.text)['access_token']

    # Step 4 -  Generate a S3 upload signature & policy
    if debug:
        print("Generate S3 Upload Signature & Policy")
    url = api_endpoint + '/v8/signature/policy'
    headers = {'content-type': 'application/json', 'authorization': 'Bearer ' + access_token}
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    if debug:
        print(r.status_code)
        print("Response: " + r.text)
    upload_response = json.loads(r.text)
    s3_upload_url = upload_response['s3UploadURL']
    s3_bucket_name = upload_response['s3BucketName']
    s3_object_key = upload_response['s3ObjectKey']
    s3_policy_document = upload_response['policyDocument']

    # Step 5 - Upload the file to S3
    if debug:
        print("Uploading File to S3")
    files = {'file': open(tmp_file_location, 'rb')}
    r = requests.post(s3_upload_url, files=files, data=s3_policy_document)
    if debug:
        print(r.status_code)
        print("Response: " + r.text)

    # Step 6 Delete to temp csv file
    if debug:
        print("Deleting temp file " + tmp_file_location)
    os.remove(tmp_file_location)

    # Step 6 - Create a dataset and triggering a deployment
    if debug:
        print("Creating Dataset to Deploy")
    url = api_endpoint + '/v8/dataSets'
    headers = {'content-type': 'application/json', 'authorization': 'Bearer ' + access_token}
    payload = {
        'name': name,
        'description': description,
        'status': 'PENDING_DEPLOYMENT',
        's3BucketName': s3_bucket_name,
        's3ObjectKey': s3_object_key,
        'fileType': 'CSV',
        'fileContainsHeader': True,
        'columnDelimiter': 'COMMA_SEPARATED',
        'columnHeaders': column_headers
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    if debug:
        print(r.status_code)
        print("Response: " + r.text)
    return json.loads(r.text)


def get_random_alphanumeric_string(length):
    letters = string.ascii_lowercase + string.digits
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
