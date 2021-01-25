drop table if exists dwb.dwb_vova_mct_rank_data;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_mct_rank_data
(
    cur_date             string COMMENT 'd_事件发生日期',
    first_cat_name       string COMMENT 'd_first_cat_name',
    rank                 string COMMENT 'd_rank',
    min_gmv              double COMMENT 'i_min_gmv',
    max_gmv              double COMMENT 'i_max_gmv',
    avg_gmv              double COMMENT 'i_avg_gmv',
    expre_uv             bigint COMMENT 'i_expre_uv',
    pay_uv               bigint COMMENT 'i_pay_uv',
    avg_avg_person_price double COMMENT 'i_avg_avg_person_price',
    avg_rep_rate_1mth    double COMMENT 'i_avg_rep_rate_1mth',
    avg_nlrf_rate_5_8w   double COMMENT 'i_avg_nlrf_rate_5_8w',
    avg_lrf_rate_9_12w   double COMMENT 'i_avg_lrf_rate_9_12w',
    expre_uv_cnt   bigint COMMENT 'i_expre_uv_cnt'
) COMMENT '商家类目等级数据'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;