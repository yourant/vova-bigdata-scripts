DROP TABLE IF EXISTS dwb.dwb_vova_mct_top1000;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_mct_top1000
(
    `datasource`                    string          COMMENT '数据平台',
    `event_date`                    date            COMMENT '日期',
    `mct_name`                      string          COMMENT '店铺名',
    `goods_on_sale_cnt`             bigint          COMMENT '在售商品数',
    `goods_hot_cnt`                 bigint          COMMENT '店铺爆品数',
    `main_cat_new`                  string          COMMENT '上新商品的第一类目',
    `main_cat_new_goods_cnt`        bigint          COMMENT '上新第一类目商品数',
    `goods_on_sale_self_cnt`        bigint          COMMENT '在售商品数（自上架）',
    `goods_new_self_cnt`            bigint          COMMENT '上新商品数（自上架）',
    `goods_on_sale_clone_cnt`       bigint         COMMENT '在售商品数（克隆）',
    `goods_new_clone_cnt`           bigint         COMMENT '上新商品数（克隆）',
    `goods_new_cnt`                 bigint          COMMENT '新上商品数',
    `operate_gap_day_cnt`           bigint          COMMENT '上次操作距今天数',
    `goods_brand_rate`              decimal(15,2)   COMMENT 'brand商品占比',
    `goods_sold_rate`               decimal(15,2)   COMMENT '商品动销率',
    `goods_new_sold_rate`           decimal(15,2)   COMMENT '新品动销率',
    `order_cnt`                     bigint          COMMENT '订单数',
    `gmv`                           decimal(15,2)   COMMENT '销售额',
    `main_cat`                      string          COMMENT '主营类目',
    `main_cat_order_cnt`            bigint          COMMENT '主营类目销量',
    `five_day_del_rate_1m`          decimal(15,2)   COMMENT '近30日5天发货率',
    `del_time_avg`                  decimal(15,2)   COMMENT '平均发货时长',
    `online_time_avg`               decimal(15,2)   COMMENT '平均上线时长',
    `flash_order_rate`              decimal(15,2)   COMMENT 'flash sale单量占比',
    `gmv_day_avg`         	        decimal(15,2)   COMMENT '本月日均gmv',
    `gmv_wow`             	        decimal(15,2)   COMMENT '每天算（前一周gmv-前二周gmv)/前二周gmv，算出来比例后保留两位小数',
    `ship_online_rate`    	        decimal(15,2)   COMMENT '7天上网率'
) COMMENT 'Top1000商家数据' PARTITIONED BY (event_date STRING)
    STORED AS PARQUETFILE;

