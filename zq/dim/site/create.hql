DROP TABLE dim.dim_zq_site;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_zq_site
(
    datasource     string,
    domain_group   string
) COMMENT '站群表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;