消息中心-推送报表v2



tmp.tmp_push_click
drop table if exists  dwb.dwb_vova_push_click_behavior_v2;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_push_click_behavior_v2
(
    datasource           string,
    push_date            date,
    platform             string,
    region_code          string,
    click_interval       string,
    target_type          string,
    r_tag                string,
    f_tag                string,
    m_tag                string,
    is_new               string,
    config_id            string,
    main_channel         string,
    push_click_uv        bigint,
    impressions          bigint,
    impressions_uv       bigint,
    impressions_pd       bigint,
    impressions_pd_uv    bigint,
    impressions_ex_pd    bigint,
    impressions_ex_pd_uv bigint,
    carts                bigint,
    carts_uv             bigint,
    orders               bigint,
    orders_uv            bigint,
    pays                 bigint,
    pays_uv              bigint,
    gmv                  bigint,
    try_num              bigint,
    push_num             bigint,
    success_num          bigint,
    job_rate             string COMMENT 'job_rate',
    brand_gmv            bigint,
    no_brand_gmv         bigint
) COMMENT '推送点击报表' PARTITIONED BY (pt STRING, intervals string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/"
;
