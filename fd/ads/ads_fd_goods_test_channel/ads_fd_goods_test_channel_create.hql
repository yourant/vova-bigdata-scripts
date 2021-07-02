create table if not exists ads.ads_fd_goods_test_channel (
    cat_name                            string COMMENT '品类名',
    selection_channel                   string COMMENT '选款渠道',
    channel_type                        string COMMENT '渠道类型',
    number_of_success_products          bigint COMMENT '成功商品数',
    number_of_end_products              bigint COMMENT '结束商品数',
    success_rate                        decimal(15,4) COMMENT '成功率',
    last_7_days_goods_sales             decimal(15,4) COMMENT '成功商品近7天销售额',
    channel_contribution                decimal(15,4) COMMENT '渠道贡献度',
    number_of_popular_products          bigint COMMENT '爆款商品数'
) comment '测款渠道数据报表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;