from flask import Flask, jsonify
from utils.aws import download_from_s3_return_df

# EB looks for an 'application' callable by default.
application = Flask(__name__)

from controllers import nz_data_controllers
    
@application.route('/test')
def get_test():
    return jsonify({'message': 'service is working'})

@application.route('/lines')
def get_lines():
    df = download_from_s3_return_df('api-data.csv', 'nz-stats/stats-api/')
    return jsonify({'message': len(df)})