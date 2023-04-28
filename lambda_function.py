import boto3
import time
import subprocess
from send_email import send_email
from datetime import datetime,timedelta
import json


def lambda_handler(event,context):

    s3_file_list = []

    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='mid-term-wh-dump')['Contents']:
        s3_file_list.append(object['Key'])
    print('s3_file_list:', s3_file_list)

    # datestr = datetime.date.today()
    tomorrow=datetime.now() + timedelta(1)
    datestr = tomorrow.strftime("%Y%m%d")

    required_file_list = [f'sales_{datestr}.csv.gz',f'inventory_{datestr}.csv.gz',f'product_{datestr}.csv.gz',
    f'store_{datestr}.csv.gz',f'calendar_{datestr}.csv.gz']

    # required_file_list = [f'sales_inv_store_wk_{datestr}.csv.gz',f'sales_inv_store_dy_{datestr}.csv.gz']


    # required_file_list=[f'sales_{datestr}.csv.gz']

    print("required_file_list:",required_file_list)


    # scan S3 bucket
    if set(s3_file_list)==set(required_file_list):
        s3_file_url = ['s3://' + 'mid-term-wh-dump/' + a for a in s3_file_list]
        print(s3_file_url)
        table_name = [a[:-16] for a in s3_file_list]
        print(table_name)

        data = json.dumps({'conf':{a: b for a,b in zip(table_name,s3_file_url)}})
        print(data)
    # send signal to Airflow
        endpoint= 'http://44.207.230.54/api/v1/dags/a_midterm_dag/dagRuns'

        subprocess.run(["curl","-X","POST", endpoint,"-H","Content-Type:application/json","--user","airflow:airflow","-d", data])
        print('File are send to Airflow')
    else:
        send_email()
