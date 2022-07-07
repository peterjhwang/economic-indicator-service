import os
import boto3
import pandas as pd
from io import StringIO

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