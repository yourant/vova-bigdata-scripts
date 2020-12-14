CREATE TABLE IF NOT EXISTS ads.ads_fd_goods_inspired
(
    batch            string comment '批次号',
    goods_id         bigint comment '商品id',
    virtual_goods_id bigint comment '虚拟商品ID',
    project_name          string comment '站点',
    country_code          string comment '国家',
    platform    string comment '平台,PC、H5、APP、others',
    like_num         bigint comment '喜欢数量',
    unlike_num       bigint comment '不喜欢数量',
    impressions      bigint comment '曝光数'
) COMMENT '每日商品喜欢情况统计,用于提供较实时的打版数据'
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;