CREATE TABLE dwd.dwd_vova_fact_buyer_portrait_base
(
    datasource   STRING,
    buyer_id     BIGINT,
    tag_id       STRING,
    tag_name     STRING,
    cnt          BIGINT,
    first_cat_id BIGINT,
    act_type_id  BIGINT
)
    PARTITIONED BY (pt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
   ;

