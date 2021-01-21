DROP TABLE dim.dim_zq_domain_userid;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_zq_domain_userid
(
    datasource  string COMMENT 'datasource',
    domain_userid  string COMMENT 'domain_userid',
    platform  string COMMENT 'platform',
    activate_time  TIMESTAMP COMMENT '激活时间',
    region_code    string COMMENT '国家',
    first_referrer string COMMENT 'first_referrer',
    first_page_url string COMMENT 'first_page_url',
    cur_buyer_id       bigint COMMENT '设备对应当前用户ID',
    first_buyer_id     bigint COMMENT '设备对应首次用户ID',
    original_channel   string COMMENT 'original_channel'
) COMMENT '站群domain_userid维表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;