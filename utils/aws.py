import os
import boto3
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
load_dotenv()

SNS_ARN = os.getenv('SNS_ARN')

# S3
def download_from_s3_return_df(filename, file_location):
    ## get cipher key
    client = boto3.client('s3')
    if not os.path.isdir('temp'):
        os.mkdir('temp')
    ## download files into "temp" folder
    client.download_file('economic-indicators', file_location+filename, 'temp/'+filename)
    ## read from "temp" folder
    with open('temp/'+filename) as f:
        lines = f.read()
    df = pd.read_csv(StringIO(lines))
    ## delete the file
    os.remove('temp/'+filename)
    return df

def upload_str_to_s3(df_str: str, file_location: str):
    client = boto3.client('s3')
    client.put_object(Body=df_str, Bucket='economic-indicators', Key=file_location)

# SNS
def send_message(typ, message):
    client = boto3.client('sns', region_name = 'ap-southeast-2')
    return client.publish(TopicArn = SNS_ARN,
                   Message = message,
                   Subject = f"{typ} - economic-indicator-service")


# Secret Manager
def get_secret(secret_name, region_name="ap-southeast-2"):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    response = client.get_secret_value(SecretId=secret_name)
    return response['SecretString']
