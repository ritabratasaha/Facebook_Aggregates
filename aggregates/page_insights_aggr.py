from datetime import *
import boto3
import pandas as pd
import s3fs
import json


def page_fans_add(fans_dict):

    """ Add up the values in the json doc """
    sumofvalues = 0
    for key, value in json.loads(fans_dict).items():
        sumofvalues = sumofvalues + value 
    return (sumofvalues)


def main():

    """ Aggregate the values for each metric at a day level for last 7 days """
    start_date_const = start_date = datetime.strptime('2020-09-12','%Y-%m-%d').date()
    end_date = start_date - timedelta(days=6)
    bucket_name = 'rs.webanalytics'
    inobj_key = 'Facebook/Csv_Store/'
    outobj_key = 'Facebook/Processed_Store/Page_Insights/' + start_date.strftime('%Y') + '/'
    page_id = '671278940149620'
    local_filename_processed = '/tmp/' + 'PageInsights_Aggr_' + page_id + '_'+ start_date.strftime('%Y%m%d') + '.csv'
    bucket_filename_processed = outobj_key + 'PageInsights_Aggr_' + page_id + '_'+ start_date.strftime('%Y%m%d') + '.csv'
    session = boto3.session.Session(profile_name='Dev')
    s3 = session.resource('s3')
    dataframe_concat = pd.DataFrame() 

    for cnt in range(7):
        
        bucket_filename = inobj_key + 'PageInsights_' + page_id + '_'+ start_date.strftime('%Y%m%d') + '.csv'
        local_filename = '/tmp/' + 'PageInsights_' + page_id + '_'+ start_date.strftime('%Y%m%d') + '.csv'

        print (bucket_filename,local_filename)
        s3.Bucket(bucket_name).download_file(bucket_filename, local_filename)

        dataframe= pd.read_csv(local_filename,
                         encoding='latin1',
                         quotechar = '"',
                         doublequote = True,
                         usecols = ['Id','Metric','Values','Period','EffectiveDate'],
                         dtype = {'Id' : str,'Metric':str,'Values':str,'Period':str,'EffectiveDate':str })
        dataframe_concat = dataframe_concat.append(dataframe)
        prev_day = start_date - timedelta(days=1)
        start_date = prev_day
  
    """Split into two data frames"""
    df_filter_wo_pagefans = dataframe_concat[(dataframe_concat.Metric != "page_fans_by_like_source") ].astype({'Values' : 'int32'})
    df_filter_w_pagefans = dataframe_concat[(dataframe_concat.Metric == "page_fans_by_like_source") ].astype({'Values' : 'str'})

    """Aggregate metrics without page fans"""
    df_group_wo_pagefans = df_filter_wo_pagefans.groupby(['Id','Metric'])
    df_sum_wo_pagefans = df_group_wo_pagefans.sum()
    df_sum_wo_pagefans['Period'] = 'week'
    df_sum_wo_pagefans['Period Start Date'] = end_date
    df_sum_wo_pagefans['Period End Date'] = start_date_const
    #print(df_sum_wo_pagefans.to_string())

    """ Aggregate metrics for page_fans_by_like_source. """
    """ First at a day level, second at a week level """
    page_fans_dict = df_filter_w_pagefans.to_dict('records')
    for each in page_fans_dict:
        txt_val = json.dumps(each["Values"])
        txt_val = eval(txt_val).replace("\'", "\"")
        json_val = json.loads(txt_val)
        if (not bool(json_val)):
           each["Values"] = 0
        else:
            sum_val =  page_fans_add(txt_val)
            each["Values"] =  sum_val
    df_page_fans = pd.DataFrame.from_dict(page_fans_dict)

    df_group_w_pagefans = df_page_fans.groupby(['Id','Metric'])
    df_sum_w_pagefans = df_group_w_pagefans.sum()
    df_sum_w_pagefans['Period'] = 'week'
    df_sum_w_pagefans['Period Start Date'] = end_date
    df_sum_w_pagefans['Period End Date'] = start_date_const
    #print (df_sum_w_pagefans.to_string())

    """ Append two data frames """
    df_sum = df_sum_wo_pagefans.append(df_sum_w_pagefans)
    print (df_sum.to_string())
    df_sum.to_csv(local_filename_processed, header=True) 
    s3.Bucket(bucket_name).upload_file(local_filename_processed, bucket_filename_processed)
    return local_filename_processed


   

if __name__ == "__main__":
    main()
