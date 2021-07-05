create external table if not exists ads.ads_fd_goods_display_top_artemis_country_interval
(
    goods_id                bigint          comment '商品ID',
    cat_id                  string          comment '类目ID',
    country_code            string          comment '国家',
    project_name            string          comment '组织',
    platform                string          comment '平台',
    start_time              timestamp       comment '开始时间',
    end_time                timestamp       comment '结束时间',
    `interval`              string          comment '标记',
    is_active               bigint          comment '默认1'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;