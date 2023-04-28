## 1. Introduction
Multiple files. We will use this architecture for multiple files. In this architecture, we use Cloudwatch to schedule Lambda to scan the S3 bucket, and send a signal to Airflow when the lambda has judged that ‘today’s’ data is ready completely. The Architecture diagram is below:

![image1](https://s3.amazonaws.com/weclouddata/images/data_engineer/architecture1.png)

This architecture mimics some companies’ ETL process: data files are sent to the companies data lake (S3 bucket) everyday from an internal transaction database, or from SFTP. The company first needs to scan the S3 bucket, when the files are ready, the ETL process will start.

For example, every day at 2 am, the data is dumped from the transaction database to S3 bucket, and when all the data has been dumped, the EMR cluster will run to process the data. And the processed data will be stored in a new S3 bucket for the next day's usage.

## 2. Details Steps
a. S3 bucket
In this step, we first dump transaction data from a transaction database. In this midterm project, we use Snowflake to pretend the transaction database. (Please note that Snowflake is a data warehouse that cannot be used as a transaction database. Here we just pretend Snowflake is a transaction database.)

Why use Snowflake? The reason we use Snowflake to pretend to be a real transaction database, like mysql, is that we only want to simulate the automatic process where raw data is dumped into the S3 bucket on schedule.

The start point of the portfolio project is the point where raw data is dumped into the S3 bucket, so it is not important to choose which database to simulate the scenario. For this reason, we use snowflake, because snowflake is easier to manage and schedule tasks for you. But, be aware that we dump raw data from a transaction database NOT a data warehouse.

In order to schedule dumping data to S3 bucket from Snowflake, we need to follow the below steps:

1). Create a Snowflake Account.
If your Snowflake account has expired, please create a new account.

2). Load data into Snowflake.
In order to mimic Snowflake as a transaction database, you need to load the transaction data first. Use below (script) to load data into Snowflake. This is a set of data, including dimension tables, store, product, calendar, and fact tables sales and inventory. This is very similar to what we used in data warehousing. The only difference is that the date is up to date. So you will have a chance to use “today’s” data.

```
CREATE DATABASE MIDTERM_DB;
CREATE SCHEMA RAW;

USE DATABASE MIDTERM_DB;
USE SCHEMA RAW;

create or replace file format csv_comma_skip1_format
type = 'CSV'
field_delimiter = ','
skip_header = 1;


create or replace stage wcd_de_midterm_s3_stage
file_format = csv_comma_skip1_format
url = 's3://weclouddata/data/de_midterm_raw/';

list @wcd_de_midterm_s3_stage;
----------------------------------------------------------------


CREATE OR REPLACE TABLE MIDTERM_DB.RAW.store
(
    store_key   INTEGER,
    store_num   varchar(30),
    store_desc  varchar(150),
    addr    varchar(500),
    city    varchar(50),
    region varchar(100),
    cntry_cd    varchar(30),
    cntry_nm    varchar(150),
    postal_zip_cd   varchar(10),
    prov_state_desc varchar(30),
    prov_state_cd   varchar(30),
    store_type_cd varchar(30),
    store_type_desc varchar(150),
    frnchs_flg  boolean,
    store_size numeric(19,3),
    market_key  integer,
    market_name varchar(150),
    submarket_key   integer,
    submarket_name  varchar(150),
    latitude    NUMERIC(19, 6),
    longitude   NUMERIC(19, 6)
);

COPY INTO MIDTERM_DB.RAW.store FROM @wcd_de_midterm_s3_stage/store_mid.csv;


CREATE OR REPLACE TABLE sales(
trans_id int,
prod_key int,
store_key int,
trans_dt date,
trans_time int,
sales_qty numeric(38,2),
sales_price numeric(38,2),
sales_amt NUMERIC(38,2),
discount numeric(38,2),
sales_cost numeric(38,2),
sales_mgrn numeric(38,2),
ship_cost numeric(38,2)
);

COPY INTO MIDTERM_DB.RAW.sales FROM @wcd_de_midterm_s3_stage/sales_mid.csv;


CREATE OR REPLACE TABLE MIDTERM_DB.RAW.calendar
(   
    cal_dt  date NOT NULL,
    cal_type_desc   varchar(20),
    day_of_wk_num    varchar(30),
    day_of_wk_desc varchar,
    yr_num  integer,
    wk_num  integer,
    yr_wk_num   integer,
    mnth_num    integer,
    yr_mnth_num integer,
    qtr_num integer,
    yr_qtr_num  integer
);

COPY INTO MIDTERM_DB.RAW.calendar FROM @wcd_de_midterm_s3_stage/calendar_mid.csv;


CREATE OR REPLACE TABLE product 
(
    prod_key int ,
    prod_name varchar,
    vol NUMERIC (38,2),
    wgt NUMERIC (38,2),
    brand_name varchar, 
    status_code int,
    status_code_name varchar,
    category_key int,
    category_name varchar,
    subcategory_key int,
    subcategory_name varchar
);

COPY INTO MIDTERM_DB.RAW.product FROM @wcd_de_midterm_s3_stage/product_mid.csv;


CREATE OR REPLACE TABLE RAW.inventory (
cal_dt date,
store_key int,
prod_key int,
inventory_on_hand_qty NUMERIC(38,2),
inventory_on_order_qty NUMERIC(38,2),
out_of_stock_flg int,
waste_qty number(38,2),
promotion_flg boolean,
next_delivery_dt date
);

COPY INTO MIDTERM_DB.RAW.inventory FROM @wcd_de_midterm_s3_stage/inventory_mid.csv;
```
3). Create a S3 bucket and S3 Stage in Snowflake.
After loading the data into Snowflake, we will prepare the S3 bucket in AWS for raw data dumping. But we cannot dump data directly into S3 bucket until we create a S3 integration Stage in Snowflake. If you forget the Staging creating process, please refer to (this link).

4). Schedule a Snowflake task to dump data to S3
When the data, S3 bucket and S3 Integration is ready, we create tasks to dump data into S3 bucket every time interval.
For Fact tables – sales and inventory, the frequency to dump the data is everyday, we can set the running every day at 2 am.

For Dimension tables – store, product and calendar, since they are not changed frequently, we usually dump data every week. But in this project, since we just mimic the procedure, we set the dimension tables to be loaded to S3 tables every day, at the same frequency as Fact tables. You can refer to below (script) to create a task.

```
--Step 1. Create a procedure to load data from Snowflake table to S3. Here, replace <your s3 stage name> with your stage name.
CREATE OR REPLACE PROCEDURE COPY_INTO_S3()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var rows = [];

    var n = new Date();
    // May need refinement to zero-pad some values or achieve a specific format
    var date = `${n.getFullYear()}-${("0" + (n.getMonth() + 1)).slice(-2)}-${n.getDate()}`;

    var st_inv = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/inventory_${date}.csv' FROM (select * from midterm_db.raw.inventory where cal_dt <= current_date()) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_sales = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/sales_${date}.csv' FROM (select * from midterm_db.raw.sales where trans_dt <= current_date()) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_store = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/store_${date}.csv' FROM (select * from midterm_db.raw.store) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_product = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/product_${date}.csv' FROM (select * from midterm_db.raw.product) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_calendar = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/calendar_${date}.csv' FROM (select * from midterm_db.raw.calendar) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });

    var result_inv = st_inv.execute();
    var result_sales = st_sales.execute();
    var result_store = st_store.execute();
    var result_product = st_product.execute();
    var result_calendar = st_calendar.execute();


    result_inv.next();
    result_sales.next();
    result_store.next();
    result_product.next();
    result_calendar.next();

    rows.push(result_inv.getColumnValue(1))
    rows.push(result_sales.getColumnValue(1))
    rows.push(result_store.getColumnValue(1))
    rows.push(result_product.getColumnValue(1))
    rows.push(result_calendar.getColumnValue(1))


    return rows;
$$;


--Step 2. Create a task to run the job. Here we use cron to set job at 2am EST everyday. 
CREATE OR REPLACE TASK load_data_to_s3
WAREHOUSE = COMPUTE_WH 
SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
CALL COPY_INTO_S3();

--Step 3. Activate the task
ALTER TASK load_data_to_s3 resume;

--Step 4. Check if the task state is 'started'
DESCRIBE TASK load_data_to_s3;
```

All the files will be labeled current_date(such as inventory_2022-08-11.csv) when they are loading to S3. You will use the date to decide which is the newest data.

If you don’t want to schedule the task automatically, you still can select different date manually with below (script ). You can manually select the data several times, each time, your selection date will be plus 1 day, in this way you still can simulate the incremental loading process.

```
----Step 1: Load today's data manually
copy into '@s3_stage/inventory_<your today's date>.csv' from (select * from midterm_db.raw.inventory where cal_dt <= current_date())
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/sales_<your today's date>.csv' from (select * from midterm_db.raw.sales where trans_dt <= current_date())
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/store_<your today's date>.csv' from (select * from midterm_db.raw.store)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/product_<your today's date>.csv' from (select * from midterm_db.raw.product)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/calendar_<your today's date>.csv' from (select * from midterm_db.raw.calendar)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;



----Step 2: Load tomorrow's data manually
copy into '@s3_stage/inventory_<your tomorrow's date>.csv' from (select * from midterm_db.raw.inventory where cal_dt <= current_date()+1)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/sales_<your tomorrow's date>.csv' from (select * from midterm_db.raw.sales where trans_dt <= current_date()+1)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/store_<your tomorrow's date>.csv' from (select * from midterm_db.raw.store)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/product_<your tomorrow's date>.csv' from (select * from midterm_db.raw.product)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/calendar_<your tomorrow's date>.csv' from (select * from midterm_db.raw.calendar)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;
```

b. Cloudwatch + Lambda
Since we have multiple files dumped to S3 separately, it is not very convenient to use S3 trigger for Lambda because we don’t know when to trigger the Lambda. Instead of using S3 event triggers for lambda, we use Cloudwatch to schedule the Lambda function.

1). Cloudwatch

We will create a Cloudwatch schedule, use Cron expression to plan when to trigger lambda (since raw data is dumped to S3 at 2 am every night, we can schedule triggers at 3 am, 3:30 am 2 times to trigger lambda) . This tutorial video will tell you how to set Cloudwatch for lambda.

2). Lambda Function

Lambda function will do several tasks:

Task 1: Scan S3 bucket to check if all ‘today’s’ files are ready, when all are ready, send a signal to airflow to start EMR; Below script is a partial Lambda function sample script to scan the S3 bucket and send single to airflow.

```
import boto3
import time
import subprocess
from send_email import send_email



s3_file_list = []

s3_client=boto3.client('s3')
for object in s3_client.list_objects_v2(Bucket='<your s3 bucket>')['Contents']:
    s3_file_list.append(object['Key'])
# print(file_list)

datestr = time.strftime("%Y-%m-%d")

required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']

# scan S3 bucket
if s3_file_list==required_file_list:
    s3_file_url = ['s3://' + '<your s3 bucket>/' + a for a in s3_file_list]
    table_name = [a[:-15] for a in s3_file_list]   

    data = json.dumps({'conf':{a:b for a in table_name for b in s3_file_url}})
# send signal to Airflow    
    endpoint= 'http://<your airflow EC2 url>/api/experimental/dags/<your airflow dag name>/dag_runs'

    subprocess.run(['curl', '-X', 'POST', endpoint, '--insecure', '--data', data])
    print('File are send to Airflow')
else:
    send_email()
 ```
 
 Task 2: If files are not ready, send an email to inform you that today’s files are not ready. The below send_email.py script is a script sample to send email. This script will be imported in the above lambda function. This is the script that leverages AWS SES service to send email. In order to do so, you still need to go to the SES console to create and verify your sender and recipient email address. The steps are very simple: go to SES, Configuration → Verified identities → Create Identity → Email address —> enter your email address —> Create identity; After this , your email is still unverified. To verify this, go to your email box, and find the verification email, verify it. The email you registered can be used. For details, you can go to this link.
 
 ```
 import boto3
from botocore.exceptions import ClientError

def send_email():
    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    SENDER = "AWS Lambda <your sender email>"

    # Replace recipient@example.com with a "To" address. If your account 
    # is still in the sandbox, this address must be verified.
    RECIPIENT = "your recipitent email address"


    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"

    # The subject line for the email.
    SUBJECT = "Files missing in S3 bucket"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Files missing in AWS S3 bucket. Please check Snowflake task.")



    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER

        )
    # Display an error if something goes wrong. 
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
```

c. Airflow
Firstly, since your Airflow is installed on EC2 instance, you need a new IAM Role for the EC2 instance to communicate with EMR cluster.Go to IAM and create the new role.

![image2](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2057.png)

![image3](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2058.png)

![image4](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2059.png)

Select AmazonEMRFullAccessPolicy_v2

![image5](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2060.png)

After the new role is created we need to go to our EC2 settings and attached the role to our EC2.

![image6](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2061.png)

![image7](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2062.png)

After Lamda sends a signal to Airflow, the Airflow will unpack the data from Lambda as the parameter for EMR. Sample airflow script sample is below:

```
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--driver-memory', '512m',
                '--executor-memory', '3g',
                '--executor-cores', '2',
                's3://<your pyspark script path>',
                '--spark_name', 'mid-term',
                '--input_bucket', "<data input bucket>",
                '--data', "{{ task_instance.xcom_pull('parse_request', key='input_paths') }}",
                '--path_output', 's3://<data output bucket>',
                '-c', 'job',
                '-m', 'append',
                '--input-options', 'header=true'
            ]
        }
    }

]

CLUSTER_ID = "j-2NT2ZIL4GUO37"

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'e,ail_on_retry': False
}

def retrieve_s3_files(**kwargs):
    data = kwargs['dag_run'].conf['data']
    kwargs['ti'].xcom_push(key = 'data', value = data)


dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default", 
    dag = dag
)

step_adder.set_upstream(parse_request)
step_checker.set_upstream(step_adder)
```

d. EMR
Write your Pyspark script to finish the follow work.

EMR is triggered by Airflow in the preceding step. EMR will use the files in the S3 bucket where raw data is dumped. Pyspark running in EMR will do the following tasks:

Task 1: Read data from S3.
Task 2: Do ETL process to generate a table to meet the business requirement.

The business requirements are the same as we did in Data Warehousing: The table will be grouped by each week, each store, each product to calculate the following metrics:

total sales quantity of a product : Sum(sales_qty)
total sales amount of a product : Sum(sales_amt)
average sales Price: Sum(sales_amt)/Sum(sales_qty)
stock level by then end of the week : stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)
store on Order level by then end of the week: ordered_stock_qty by the end of the week (only the ordered stock quantity at the end day of the week)
total cost of the week: Sum(cost_amt)
the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)
total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg)
potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)
no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)
low Stock Instances: Calculate how many times of Low_Stock_Flg in a week
no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week
how many weeks the on hand stock can supply: (stock_on_hand_qty at the end of the week) / sum(sales_qty)
Task 3: Save the final fact table and dimension tables to as parquet files in a new S3 bucket, this bucket will be used for data analysis.

In addition, you can also refer to this package which provide you a more professional and complicated Pyspark scripts framework.

e. Athena and Glue
Connect Athena and Glue with the S3 bucket storing your final fact and dimension tables.

f. BI tools (Superset)
Use a BI tool to connect to Athena to generate visualized reports for the project as the final steps. You either can use Superset as your BI tool, or you can choose other BI tools you are familiar with, such as Power BI or Tableau, to do data analysis.

The below steps are the steps on how to use Superset.

Docker superset image is located in Docker Hub → https://hub.docker.com/r/apache/superset. We need to modify this image to include Athena connector. Create a new Dockerfile with the following commands and create an updated superset image.

```
FROM apache/superset:9fe02220092305ca8b24d4228d9ab2b6146afed6

USER root
RUN pip install "PyAthena>1.2.0"

USER superset
```

Alternatively you can use prebuilt image that you can pull from here → docker pull stantaov/superset-athena:0.0.1

After pulling the image deploy superset image by running the following command.

```
docker run -d -p 8080:8088 --name superset apache/superset

# or if you want to use prebuit image

docker run -d -p 8088:8088 --name superset stantaov/superset-athena:0.0.1
```

With your local superset container already running setup your local admin account

```
docker exec -it superset superset fab create-admin \
               --username admin \
               --firstname Superset \
               --lastname Admin \
               --email admin@superset.com \
               --password admin
               
               ```
 
 Update local DB to latest
 
 ```
 docker exec -it superset superset db upgrade
 ```
 
 Load Examples (Optional)
 ```
 docker exec -it superset superset load_examples
 ```
 
 Setup roles
 ```
 docker exec -it superset superset init
 ```
 
 Login and take a look -- navigate to http://:8080/login/ -- u/p: [admin/admin]

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2091.png)

Now we need to create a new database connection to connect Superset with Athena. Go to Data → Databases and click on “+ Database” and provide database name and SQLALCHEMY URI.

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2092.png)

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2093.png)

Next we need to provide the following connection string:

```
awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com/{schema_name}?s3_staging_dir={s3_staging_dir}&work_group={work_group_name}
```

Where ....

awsathena+rest - Recommended SQL Alchemy driver

{aws_access_key_id} and {aws_secret_access_key} - Access key ID and Secret access key an IAM user for this project.

{region_name} - Region of your Athena and S3 bucket (should match)

{schema_name} - Athena table name

{s3_staging_dir} - The URI of the S3 bucket where the Athena data is located (the location of the data used for Glue crawler)

{work_group_name} -The name of the workgroup associated with the Athena data (primary is default)

Example used for this project

```
awsathena+rest://XXXXXXXXX:XXXXXXXXXXX@athena.us-east-1.amazonaws.com/midterm-project?s3_staging_dir=s3://weclouddata-de/output/&work_group=primary

```

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2094.png)

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2095.png)

After adding the database we need to select dataset for the midterm-project database.

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2096.png)

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2097.png)

![image](https://s3.amazonaws.com/weclouddata/images/data_engineer/midterm_stan/Untitled%2098.png)

Now you will be able to build charts, dashboards or run SQL queries in Superset against the midterm-project.output dataset.



