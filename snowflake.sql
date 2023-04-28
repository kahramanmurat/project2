CREATE WAREHOUSE MIDTERM_WH WITH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60;
USE WAREHOUSE MIDTERM_WH;

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



create storage integration s3_int
  type = external_stage
  storage_provider = 'S3'
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::963656558707:role/snowflake-access-role'
  storage_allowed_locations = ('s3://mid-term-wh-dump/');
  
DESC INTEGRATION s3_int;


create stage s3_stage
  storage_integration = s3_int
  url = 's3://mid-term-wh-dump'
  file_format = csv_comma_skip1_format;
  
list @s3_stage;


----Step 1: Load today's data manually
copy into '@s3_stage/inventory_20221012.csv.gz' from (select * from midterm_db.raw.inventory where cal_dt <= current_date())
file_format=(TYPE=CSV, COMPRESSION='GZIP')
HEADER=TRUE
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/inventory_20221012.csv.gz' from (select * from midterm_db.raw.inventory where cal_dt <= '2022-10-11')
file_format=(TYPE=CSV, COMPRESSION='GZIP')
HEADER=TRUE
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;


copy into '@s3_stage/sales_20221012.csv.gz' from (select * from midterm_db.raw.sales where trans_dt <= current_date())
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
HEADER=TRUE
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/sales_20221012.csv.gz' from (select * from midterm_db.raw.sales where trans_dt <= '2022-10-12')
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
HEADER=TRUE
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/store_20221012.csv.gz' from (select * from midterm_db.raw.store)
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER=TRUE
;

copy into '@s3_stage/store_20221012.csv.gz' from (select * from midterm_db.raw.store)
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER=TRUE
;

copy into '@s3_stage/product_20221012.csv.gz' from (select * from midterm_db.raw.product)
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER=TRUE
;

copy into '@s3_stage/product_20221012.csv.gz' from (select * from midterm_db.raw.product)
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER=TRUE
;

copy into '@s3_stage/calendar_20221012.csv.gz' from (select * from midterm_db.raw.calendar)
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER=TRUE
;


copy into '@s3_stage/calendar_20221012.csv.gz' from (select * from midterm_db.raw.calendar)
file_format=(TYPE=CSV, COMPRESSION='GZIP')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER=TRUE
;

