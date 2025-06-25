declare v_uri string;
declare v_datadt date;
declare v_datadtraw string;
declare dynamicsql string;
--set v_uri='gs://incpetez-data-samples/dataset/bqdata/ext_src_data/custs_header_20230908';
set v_uri=@param1;
set v_datadtraw=(select right(v_uri,8));
set v_datadt=(select parse_date('%Y%m%d',v_datadtraw));
--combining 3 different strings to make it as a single string by adding uri in the middle.
set dynamicsql=CONCAT('CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext ( custno INT64,firstname STRING,lastname STRING,age INT64,profession STRING) OPTIONS (  format = "CSV", uris = ["',v_uri, '"],max_bad_records = 2, skip_leading_rows=1)');

begin

--CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext ( custno INT64,firstname STRING,lastname STRING,age INT64,profession STRING) OPTIONS (  format = "CSV", uris = ["gs://incpetez-data-samples/dataset/bqdata/ext_src_data/custs_header_20230908"],max_bad_records = 2, skip_leading_rows=1);

--executing the single dynamic sql query string built in the above statement as a sql query using execute immediate function.
EXECUTE IMMEDIATE dynamicsql;

--SCD2 Implementation (maintain the history and loading new data without affecting the history)
create table if not exists curatedds.cust_part_curated (custno INT64,name STRING,
age INT64,profession STRING,datadt date) partition by datadt;

delete from curatedds.cust_part_curated where datadt=v_datadt;

Insert into curatedds.cust_part_curated
select custno, concat(firstname,",", lastname) as name,age, coalesce(profession,'na') as profession,v_datadt from rawds.cust_ext;

select datadt,COUNT(1) from curatedds.cust_part_curated  GROUP BY DATADT limit 10;

--SCD1 - If We don't want to maintain the history, we can use merge statement 

create table if not exists curatedds.cust_part_curated_merge (custno INT64,name STRING,
age INT64,profession STRING,datadt date) partition by datadt;

MERGE `curatedds.cust_part_curated_merge` T
USING (SELECT custno, concat(firstname,",", lastname) as name,age, coalesce(profession,'na') profession,cast(v_datadt as date) datadt FROM rawds.cust_ext) S
ON T.custno = S.custno
WHEN MATCHED THEN
UPDATE SET T.name = S.name,T.age = S.age,T.profession = S.profession,T.datadt=S.datadt
WHEN NOT MATCHED THEN
INSERT (custno,name,age,profession,datadt) VALUES (S.custno,S.name,S.age,S.profession,S.datadt);

select datadt,COUNT(1) from curatedds.cust_part_curated_merge  GROUP BY DATADT limit 10;

end;