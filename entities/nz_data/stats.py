from utils.aws import download_from_s3_return_df
from flask_app import application
from datetime import datetime as dt
import pandas as pd

class CacheData:
    def __init__(self):
        self.load_data()

    def load_data(self):
        now = dt.now()
        data_df = download_from_s3_return_df('api-data.csv', 'nz-stats/stats-api/')
        data_df = data_df[(data_df['ResourceID'].isin(['CPTRD2', 'CPTRD4', 'CPTRD1', 'CPTRD5'])) | (data_df['Duration'].notnull())].copy()
        data_df['Period'] = pd.to_datetime(data_df['Period'])
        data_df.fillna('', inplace=True)

        meta_df = download_from_s3_return_df('meta.csv', 'nz-stats/stats-api/')
        meta_df.fillna('', inplace=True)

        temp_df = data_df.merge(meta_df, on='ResourceID')
        self.df = temp_df.groupby(['Subject', 'ResourceID', 'Title', 'Measure', 'Duration', 'Period', 'GeoUnit', 'Geo', 'Label1', 'Label2', 'Label3', 'Unit', 'Multiplier'])[['Value']].sum().reset_index()
        self.df.loc[self.df.Duration=='P1D', 'Duration'] = 1
        self.df.loc[self.df.Duration=='P7D', 'Duration'] = 7
        self.df.loc[self.df.Duration=='P1M', 'Duration'] = 30
        self.df.loc[self.df.Duration=='P3M', 'Duration'] = 90
        application.logger.info(f'{len(self.df)} stats data loaded', dt.now() - now)

    def get_dataframe(self):
        return self.df

    def get_table(self, cat, title, is_regional, is_industry):
        temp = self.df
        category_filter = temp['Subject']!='' if pd.isnull(cat) or cat == 'All' else temp['Subject'] == cat
        title_filter = temp['Title']!='' if pd.isnull(title) or title == 'All' else temp['Title'] == title
        regional_filter = temp['GeoUnit']!='' if is_regional else temp['GeoUnit']==''
        industry_filter = temp['Title'].str.contains('industry', case=False) if is_industry else True
        temp = temp[category_filter & title_filter & regional_filter & industry_filter]
        return temp[['Title', 'Label1', 'Label2', 'Label3', 'Geo', 'Unit']].drop_duplicates().to_dict('records')            


    def get_subjects(self):
        temp = self.df
        return temp['Subject'].unique().tolist()

    def get_titles(self):
        temp = self.df
        return temp['Title'].unique().tolist()

    def get_titles_catagory(self, cat, is_regional, is_industry):
        temp = self.df
        regional_filter = temp['GeoUnit'].notnull() if is_regional else temp['GeoUnit'].isnull()
        industry_filter = temp['Title'].str.contains('industry', case=False) if is_industry else True
        temp = temp[regional_filter & industry_filter].copy()
        if cat == 'All':
            return temp['Title'].unique().tolist()
        else :
            temp = temp[temp['Subject']==cat]
            return temp['Title'].unique().tolist()

def get_groupby_cal():
    cal_temp_df = download_from_s3_return_df('data_cal.csv', 'nz-stats/stats-api/')
    cal_temp_df.set_index('Title', inplace=True)
    return cal_temp_df['Calculation'].to_dict()