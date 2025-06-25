--drop table rawds.trans_pos;
--drop table rawds.trans_online;
--drop table rawds.consumer;
--drop table rawds.trans_pos;
--drop table rawds.trans_mobile_channel;
--drop table curatedds.trans_online_part;
--drop table curatedds.trans_pos_part_cluster;
--drop table `curatedds.trans_mobile_autopart_2021`;
--drop table `curatedds.trans_mobile_autopart_2022`;
--drop table `curatedds.trans_mobile_autopart_2023`;


select current_timestamp,"Load started";
select current_timestamp,"Create and Load CSV data into BQ Managed table with defined schema";
LOAD DATA OVERWRITE `rawds.trans_pos` (txnno numeric,txndt string,custno int64,amt float64,category string,product string,city string, state string, spendby string)
  FROM FILES (
    format = 'CSV', uris = ['gs://incpetez-data-samples/dataset/bqdata/store_pos_product_trans.csv'],
    field_delimiter=',');

select current_timestamp,"Create and Load JSON data into BQ Managed table using auto detect schema";
LOAD DATA OVERWRITE rawds.trans_online
  FROM FILES (
    format = 'JSON', uris = ['gs://incpetez-data-samples/dataset/bqdata/online_products_trans.json']);

select current_timestamp,"Create manually the table and Load CSV data into BQ Managed table, skip the header column in the file";

create table if not exists rawds.consumer(
custno INT64, 
firstname STRING,
lastname STRING,
age INT64,
profession STRING);

LOAD DATA OVERWRITE rawds.consumer
  FROM FILES (
    format = 'CSV', uris = ['gs://incpetez-data-samples/dataset/bqdata/custs_header'],
    skip_leading_rows=1,field_delimiter=',');

select current_timestamp,"Create the table using auto detect schema using the header column and Load CSV data into BQ Managed table";

LOAD DATA OVERWRITE `rawds.trans_mobile_channel` 
  FROM FILES (
    format = 'CSV', uris = ['gs://incpetez-data-samples/dataset/bqdata/mobile_trans.csv'],
    field_delimiter=',');

select current_timestamp,"Load completed Successfully";