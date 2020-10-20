import boto3
from datetime import *
import pandas as pd



bucket_name = 'rs.webanalytics'
inobj_key = 'Facebook/Csv_Store/'
outobj_key = 'Facebook/Processed_Store/'
page_id = '671278940149620'
start_date = startdate = datetime.strptime('2020-08-29','%Y-%m-%d').date()
end_date = startdate - timedelta(days=1)
session = boto3.session.Session(profile_name='Dev')
s3 = session.resource('s3')


dataframe_concat = pd.DataFrame() 


for cnt in range(1):
    
    bucket_filename = inobj_key + 'PageInsights_' + page_id + '_'+ startdate.strftime('%Y%m%d') + '.csv'
    local_filename = '/tmp/' + 'PageInsights_' + page_id + '_'+ startdate.strftime('%Y%m%d') + '.csv'
    s3.Bucket(bucket_name).download_file(bucket_filename, local_filename)

    print(bucket_filename,local_filename)
    dataframe= pd.read_csv(local_filename,
                         encoding='latin1',
                         quotechar = '"',
                         doublequote = True,
                         usecols = ['Id','Metric','Values','Period','EffectiveDate'],
                         dtype = {'Id' : str,'Metric':str,'Values':str,'Period':str,'EffectiveDate':str })
    dataframe_concat = dataframe_concat.append(dataframe)
    prev_day = startdate - timedelta(days=1)
    startdate = prev_day

print(dataframe_concat.to_string())

df_filter = dataframe_concat[(dataframe_concat.Metric != "page_fans_by_like_source") ].astype({'Values' : 'int32'})
df_group = df_filter.groupby(['Id','Metric'])

print(df_group.sum().to_string())

df_sum = df_group.sum()
df_sum['Period'] = 'week'
df_sum['Period Start Date'] = end_date
df_sum['Period End Date'] = start_date

print(df_sum.to_string())
