drop table if exists dwb.dwb_vova_mct_rank_data;
CREATE TABLE dwb.dwb_vova_mct_rank_data(
  cur_date string,
  first_cat_name string,
  rank string,
  min_gmv double,
  max_gmv double,
  avg_gmv double,
  expre_uv bigint,
  pay_uv bigint,
  avg_avg_person_price double,
  avg_rep_rate_1mth double,
  avg_nlrf_rate_5_8w double,
  avg_lrf_rate_9_12w double,
  expre_uv_cnt bigint)
COMMENT '商家类目等级数据'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;