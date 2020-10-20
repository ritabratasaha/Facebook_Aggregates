import boto3


session = boto3.session.Session(profile_name='Dev')
s3 = session.client('s3')

response = s3.head_object(
    Bucket='rs.webanalytics',
    Key='Facebook/Csv_Store/PageInsights_671278940149620_20200810.csv',
)

print(response)