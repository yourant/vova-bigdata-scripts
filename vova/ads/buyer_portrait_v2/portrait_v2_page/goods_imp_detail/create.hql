-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_goods_imp_detail;
create table if  not exists  ads.ads_vova_goods_imp_detail (
    `goods_id`                      bigint        COMMENT 'd_商品id',
    `page_code`                     string        COMMENT 'd_page_code',
    `list_type`                     string        COMMENT 'd_list_type',
    `rp_name`                       string        COMMENT 'd_rp_name',
    `expre_cnt`                     int           COMMENT 'i_曝光数',
    `clk_cnt`                       int           COMMENT 'i_点击数',
    `add_cart_cnt`                  int           COMMENT 'i_加车数',
    `order_cnt`                     int           COMMENT 'i_订单数',
    `gmv`                           decimal(13,2) COMMENT 'i_gmv'
)comment '页面展示查商品曝光明细' PARTITIONED BY (pt string,gpt string)
     STORED AS PARQUETFILE;