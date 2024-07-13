CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
    URL='s3://snowflake-real-time-streaming/stream_data/'
    CREDENTIALS = (AWS_KEY_ID='ENTER-AWS-KEY', AWS_SECRET_KEY='ENTER-AWS-SECRET-KEY');

CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
    TYPE = CSV,
    FIELD_DELIMITER = ",",
    SKIP_HEADER = 1;

LIST @customer_ext_stage;

CREATE OR REPLACE PIPE customer_s3_pipe
    AUTO_INGEST = TRUE 
    AS 
    COPY INTO customer_raw
    FROM @customer_ext_stage
    FILE_FORMAT = CSV

SHOW PIPES;



SELECT COUNT(*)
FROM CUSTOMER_RAW;

TRUNCATE CUSTOMER_RAW;
