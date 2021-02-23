-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_buyer_page_goods_top_behave;
create external table if  not exists ads.ads_vova_buyer_page_goods_top_behave  (
    `buyer_id` BIGINT COMMENT 'd_买家id',
    `datasource` STRING COMMENT 'i_datasource',
    `device_id` STRING COMMENT 'i_用户设备号',
    `email` STRING COMMENT 'i_用户邮箱',
    `type` STRING COMMENT 'i_类型(click_1m,add_cat_2m,order_6m)',
    `behave_top_array` ARRAY<STRING> COMMENT 'i_top20行为array'
    )COMMENT '用户品类点击、加购、订单top行为记录' PARTITIONED BY (bpt string)
    STORED AS PARQUETFILE;