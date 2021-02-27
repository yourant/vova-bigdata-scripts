-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_buyer_behave_track;
create external table if  not exists  ads.ads_vova_buyer_behave_track (
    `buyer_id`                    bigint        COMMENT 'd_买家id',
    `type`                        string        COMMENT 'i_事件类型(screenview,click,data)',
    `event_type`                  string        COMMENT 'i_打点类型(normal,goods,null)',
    `page_code`                   string        COMMENT 'i_page_code',
    `element_name`                string        COMMENT 'i_element_name',
    `goods_id`                    string        COMMENT 'i_商品id',
    `event_time`                  timestamp     COMMENT 'i_事件时间'
)comment '用户行动轨迹' PARTITIONED BY (pt string,bpt string)
     STORED AS PARQUETFILE;

