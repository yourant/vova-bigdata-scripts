-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_buyer_page_rating_top;
create external table if  not exists  ads.ads_vova_buyer_page_rating_top (
    `buyer_id`                    bigint        COMMENT 'd_买家id',
    `datasource`                  string        COMMENT 'i_datasource',
    `device_id`                   string        COMMENT 'i_用户设备号',
    `email`                       string        COMMENt 'i_用户邮箱',
    `goods_id`                    bigint        COMMENT 'i_商品id',
    `goods_thumb`                 string        COMMENT 'i_图片路径',
    `rating`                      decimal(13,4) COMMENT 'i_rating'
)comment 'rating top 30' PARTITIONED BY (bpt string)
     STORED AS PARQUETFILE;