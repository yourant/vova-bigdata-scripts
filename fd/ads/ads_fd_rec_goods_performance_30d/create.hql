create table if not exists ads.ads_fd_goods_performance_30d
(
    goods_id         bigint,
    click            bigint comment "点击次数",
    impression       bigint comment "展示次数",
    sales            bigint comment "下单销量，不区分订单状态"
) comment "商品30天表现数据，不区分组织"
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;