import os
import json
import boto3
import time

bucket_name = 'mp10-s3'
folder_names= ["output/part_a/", "output/part_b/", "output/part_c/"]
def lambda_handler(event, context):
    res = {}
    for i, name in enumerate(folder_names):
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=name)
        
        names=[obj['Key'] for obj in response.get('Contents', [])]
        #print(names)
        
        if names:
            modif_data = []
            for file in names:
                s3_response = s3_client.get_object(Bucket=bucket_name, Key=file)
                modif_data.append(s3_response['Body'].read().decode('utf-8').strip())
            res[i] = '\n'.join(modif_data)
        else:
            print("No data found in S3 folder '%s'" % (name))
            res[i]={}
    print("DEBUG – about to return:", json.dumps(res))
    return res