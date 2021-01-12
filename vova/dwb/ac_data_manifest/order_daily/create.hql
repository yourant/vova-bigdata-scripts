-- [5359]—AC日报表
-- https://zt.gitvv.com/index.php?m=task&f=view&taskID=21432
-- 任务描述
-- PRD地址： https://tqqmh6.axshare.com
--
-- 数据源：AiryClub
-- 国家筛选：all，gb，fr，de，it，es
-- 渠道：all，google，facebook
--
-- 今日营收GMV
-- 昨日营收GMV
-- 新客GMV：新激活用户在当日产生的GMV
-- 当日总GMV
-- 主流程支付订单uv
-- 加购成功uv
-- dau
-- 该日产生的订单在7日内返回物流揽收的订单数
-- 该日总订单数
-- 当日产生的收货前取消的订单总数
-- 当日产生的收货后退货退款订单（且审核通过）的订单数

CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_ac_order_daily (
datasource                    string   COMMENT 'd_datasource',
region_code                   string   COMMENT 'd_国家/地区',
main_channel                  string   COMMENT 'd_主渠道',

gmv                              decimal(10,4)   COMMENT 'i_当日营收GMV',
new_buyer_gmv                    decimal(10,4)   COMMENT 'i_新客GMV',
pay_uv                           bigint          COMMENT 'i_主流程支付订单uv',
order_goods_cnt                  bigint          COMMENT 'i_该日总订单数',
shop_online_day7_order_goods_cnt bigint          COMMENT 'i_该日产生的订单在7日内返回物流揽收的订单数',
yesterday_gmv                    decimal(10,4)   COMMENT 'i_昨日营收GMV',
add_cart_uv                      bigint          COMMENT 'i_加购成功uv',
dau                              bigint          COMMENT 'i_DAU',
no_sign_refund_order_goods_cnt   bigint          COMMENT 'i_当日产生的收货前取消的订单总数',
sign_refund_order_goods_cnt      bigint          COMMENT 'i_当日产生的收货后退货退款订单（且审核通过）的订单数'
) COMMENT 'AC日报表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_ac_order_daily/"
;