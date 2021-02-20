-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_buyer_page_category_top_behave;
create external table if  not exists  ads.ads_vova_buyer_page_category_top_behave (
    `buyer_id`                    bigint        COMMENT 'd_买家id',
    `datasource`                  string        COMMENT 'i_datasource',
    `device_id`                   string        COMMENT 'i_用户设备号',
    `email`                       string        COMMENt 'i_用户邮箱',
    `type`                        string        COMMENT 'i_类型(click,add_cat,order)',
    `day_gap`                     string        COMMENT 'i_间隔时间',
    `behave_top_array`            Array<string> COMMENT 'i_top20行为array'
)comment '用户品类点击、加购、订单top行为记录' PARTITIONED BY (bpt string)
     STORED AS PARQUETFILE;