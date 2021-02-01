drop table dwb.dwb_vova_web_rec_report;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_web_rec_report
(
event_date                 date     COMMENT '事件发生日期',
datasource                 string   COMMENT 'datasource',
country                    string   COMMENT '国家',
platform                    string   COMMENT 'pc|web',
rec_page_code              string   COMMENT '页面',
page_code                  string   COMMENT '页面',
list_type                  string   COMMENT 'list_type',
original_channel           string   COMMENT 'original_channel',
expres                     bigint   COMMENT '曝光数',
clks                       bigint   COMMENT '点击数',
clk_uv                     bigint   COMMENT '点击uv',
expre_uv                   bigint   COMMENT '曝光uv',
cart_uv                    bigint   COMMENT '加购uv',
order_number               bigint   COMMENT '订单数',
payed_number               bigint   COMMENT '支付单数',
payed_uv                   bigint   COMMENT '支付uv',
gmv                        decimal(15, 4)  COMMENT 'gmv',
cart_uv_div_expre_uv       decimal(15, 4)  COMMENT '加购率',
payed_uv_div_expre_uv      decimal(15, 4)  COMMENT '支付转化率',
gmv_mom                    decimal(15, 4)  COMMENT 'gmv环比',
payed_uv_div_expre_uv_mom  decimal(15, 4)  COMMENT '支付转化率环比',
cart_uv_div_expre_uv_mom  decimal(15, 4)  COMMENT '加车率环比'
)COMMENT 'dwb_vova_web_rec_report'
PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
