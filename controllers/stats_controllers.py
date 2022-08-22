from flask import request, jsonify
from flask_app import application
from entities.nz_data.stats import CacheData, get_groupby_cal
from utils.aws import send_message
from datetime import datetime as dt

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import pandas as pd

DEFAULT_COLORS = px.colors.qualitative.Alphabet

data_obj = CacheData()
selected_metric_dict = dict()

def reload_stats_data():
    try:
        data_obj.load_data()
        #send_message("INFO", 'Cache file successfully reloaded.\n\neconomic-indicator-service')
        return True
    except Exception as e:
        send_message("ERROR", str(e) + '\n\neconomic-indicator-service')
        return False

@application.route('/reload')
def reload_data():
    if reload_stats_data():
        return jsonify({'message': 'Successfully data reloaded'})
    else:
        return jsonify({'Error': 'Data reload failed'})

SYMBOL_DICT = {1: 'D',
               7: 'W',
               30: 'M',
               90: 'Q'
}
AGG_LEVEL_DICT = {1: 'daily',
               7: 'weekly',
               30: 'monthly',
               90: 'quarterly'
}
GROUP_BY_DICT = get_groupby_cal()

def manipulate_data(metrics, date_range_min, date_range_max):
    # find max duration
    max_duration = 0
    for metric in metrics:
        temp_dur = data_obj.df.loc[(data_obj.df['Title']==metric['Title'])
                    & (data_obj.df['Geo']==metric['Geo'])
                    & (data_obj.df['Label1']==metric['Label1'])
                    & (data_obj.df['Label2']==metric['Label2'])
                    & (data_obj.df['Label3']==metric['Label3']), 'Duration'].min()
        if max_duration < temp_dur:
            max_duration = temp_dur
    result_df = pd.DataFrame()
    application.logger.info(max_duration)
    application.logger.info(SYMBOL_DICT)
    resample_by = SYMBOL_DICT[max_duration]
    # re-create dataframe
    secondary_y_dict = dict()
    for metric in metrics:
        col_name = metric['Title'] + ' ' + metric['Geo'] + ' ' + metric['Label1'] + ' ' + metric['Label2'] + ' ' + metric['Label3']
        if col_name not in secondary_y_dict.keys():
            secondary_y_dict[col_name] = metric['secondary_y']
            temp = data_obj.df.loc[(data_obj.df['Title']==metric['Title'])
                    & (data_obj.df['Geo']==metric['Geo'])
                    & (data_obj.df['Label1']==metric['Label1'])
                    & (data_obj.df['Label2']==metric['Label2'])
                    & (data_obj.df['Label3']==metric['Label3'])
                    & (data_obj.df['Period']>=date_range_min)
                    & (data_obj.df['Period']<date_range_max)].copy()
            calculation = 'sum' if pd.isnull(GROUP_BY_DICT.get(metric['Title'])) else GROUP_BY_DICT.get(metric['Title'])
            print(metric['Title'], calculation)
            temp = temp.groupby('Period').agg({'Value': calculation}) #[['Value']].sum()
            temp.columns = [col_name]
            temp = temp.resample(resample_by).agg({col_name: calculation})
            result_df = result_df.join(temp, how='outer')
    return result_df, f"(Aggregated at a {AGG_LEVEL_DICT[max_duration]} level)", secondary_y_dict

@application.route('/get_timeframe')
def get_timeframe():
    application.logger.info('/get_timeframe')
    return jsonify({'min': data_obj.df.Period.min(),
        'max': data_obj.df.Period.max(),
    })

@application.route('/get_table', methods=['POST'])
def get_table():
    req = request.get_json()
    cat = req['category']
    title = req['title']
    is_regional = req['regional']
    is_industry = req['industry']
    application.logger.info('/get_table')
    return jsonify({'data': data_obj.get_table(cat, title, is_regional, is_industry)})

@application.route('/get_titles')
def get_titles():
    application.logger.info('/get_titles')
    return jsonify({'data': data_obj.get_titles()})

@application.route('/get_titles', methods=['POST'])
def get_titles_catagory():
    cat = request.get_json()['category']
    is_regional = request.get_json()['regional']
    is_industry = request.get_json()['industry']
    application.logger.info('/get_titles')
    return jsonify({'data': data_obj.get_titles_catagory(cat, is_regional, is_industry)})

@application.route('/get_subjects')
def get_subjects():
    application.logger.info('/get_subjects')
    return jsonify({'data': data_obj.get_subjects()})

@application.route('/reset_metric', methods=['POST'])
def reset_metric():
    global selected_metric_dict
    application.logger.info('/reset_metric')
    cookie = request.get_json()['cookie']
    selected_metric_dict[cookie] = []
    return jsonify({'message': 'successfully reset'})

@application.route('/add_metric', methods=['POST'])
def add_metric():
    global selected_metric_dict
    application.logger.info('/add_metric')
    cookie = request.get_json()['cookie']
    if cookie in selected_metric_dict.keys():
        selected_metric_dict[cookie] += request.get_json()['metrics']
    else:
        selected_metric_dict[cookie] = request.get_json()['metrics']
    application.logger.info(len(selected_metric_dict[cookie]))
    return jsonify({'message': 'successfully added'})

@application.route('/get_graphs', methods=['POST'])
def get_graphs():
    application.logger.info('/get_graphs')
    cookie = request.get_json()['cookie']
    date_range_min = request.get_json()['min']
    date_range_max = request.get_json()['max']
    if cookie not in selected_metric_dict.keys():
        return jsonify({'chart1': go.Figure().to_json(),
                'chart2': go.Figure().to_json(),
                'agg-level': ''})
    metrics = selected_metric_dict[cookie]
    if len(metrics)==0:
        return jsonify({'chart1': go.Figure().to_json(),
                'chart2': go.Figure().to_json(),
                'agg-level': ''})
    #print(titles)
    chart1_fig = make_subplots(specs=[[{"secondary_y": True}]])
    dff, agg_level, secondary_y_dict = manipulate_data(metrics, date_range_min, date_range_max)
    for col in dff.columns:
        chart1_fig.add_trace(go.Scatter(x= dff.index,
                                 y= dff[col],
                                 name= col), secondary_y= secondary_y_dict[col])
    chart1_fig.update_layout(height=450,
        hovermode='x unified',
        margin=dict(l=20, r=20, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))
    chart2_fig = px.imshow(dff.corr(), text_auto=True, aspect="auto")
    chart2_fig.update_layout(height=350, margin=dict(l=10, r=10, b=10), font=dict(size=9))
    chart2_fig.update_xaxes(tickangle=45)#visible=False
    return jsonify({'chart1': chart1_fig.to_json(),
                'chart2': chart2_fig.to_json(),
                'agg-level': agg_level})

@application.route('/get_csv', methods=['POST'])
def get_csv():
    application.logger.info('/get_csv')
    application.logger.info(request.get_json())
    cookie = request.get_json()['cookie']
    if cookie in selected_metric_dict:
        metrics = selected_metric_dict[cookie]
        date_range_min = request.get_json()['min']
        date_range_max = request.get_json()['max']
        #print(titles)
        fig = go.Figure()
        count = 0
        dff, _, _ = manipulate_data(metrics, date_range_min, date_range_max)
        return jsonify({'data': dff.to_csv(), 
                'filename': f"nz_economic_data_{dt.now().strftime('%Y%m%d')}.csv"})
    else:
        return jsonify({'data': 'no data'})