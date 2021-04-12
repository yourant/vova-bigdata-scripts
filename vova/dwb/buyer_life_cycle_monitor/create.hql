[8070]用户生命周期监控报表
https://zt.gitvv.com/index.php?m=task&f=view&taskID=30433
需求目的：
1、了解用户在不同阶段下的LTV，评估新客留存项目效果；
2、了解激活用户在不同阶段的转化情况和下单情况，掌握平台用户转化和购买频次等信息；

需求内容：
https://docs.google.com/spreadsheets/d/19T8u5FUpg_AimNmB_880I-3R0IgdTJgy0-CZm-JmVOg/edit#gid=2010163347
需求内容
dashboard名: 用户生命周期监控报表
维度说明
日期
国家  TOP10国家/others/all  TOP10国家待提供
组织  vova/ac/app群/all
平台  ios/android/all
渠道  main_channel：仅需要all/Facebook Ads/googleadwords_int/organic/other 5种

表名  字段名 字段业务口径（定义、公式或逻辑）
表1：用户LTV表现报表
    当日补贴成本    当日激活用户中在当日下单的用户所用补贴之和/当天新激活下单用户数
    30天补贴成本    当日激活用户中在0-30天内下单的用户所用补贴之和/当天新激活下单用户数
    当天LTV        当天GMV/当日激活用户数
    3天LTV         3天GMV/当日激活用户数
    7天LTV         7天GMV/当日激活用户数
    14天LTV
    28天LTV
    60天LTV
    90天LTV
    180天LTV
    当天激活用户数  device_id去重，不同组织之间有相同device_id时也应去重
    当天GMV
    3天GMV
    7天GMV
    14天GMV
    28天GMV
    60天GMV
    90天GMV
    180天GMV
"“n天GMV”是统计当日激活用户在未来一段时间内产生的GMV。
自用户激活之日当日(第0天)算起，用户在0至n天内产生的总GMV，计为n天GMV。
当现有数据不足n天时，直接计为NA，如2020/12/31的90日GMV应为NA"

表2：用户转化率表现报表
    当天转化率           当天新激活下单用户数/当日激活用户数
    3天转化率            3天新激活下单用户数/当日激活用户数
    7天转化率            7天新激活下单用户数/当日激活用户数
    14天转化率
    28天转化率
    60天转化率
    90天转化率
    180天转化率
    当天新激活下单用户数   当天激活用户中在当天下单的用户数
    3天下单用户数         当天激活用户中在0-3天内下单的用户数
    7天下单用户数         当天激活用户中在0-7天内下单的用户数
    14天下单用户数
    28天下单用户数
    60天下单用户数
    90天下单用户数
    180天下单用户数

表3：订单表现报表
    当天平均订单数    当天订单数/当天新激活下单用户数
    3天平均订单数     3天订单数/3天新激活下单用户数
    7天平均订单数     7天订单数/7天新激活下单用户数
    14天平均订单数
    28天平均订单数
    60天平均订单数
    90天平均订单数
    180天平均订单数
    当天订单数       当天激活用户中在当天下单的用户的下单数量之和
    3天订单数        当天激活用户中在0-3天内下单的用户的下单数量之和
    7天订单数        当天激活用户中在0-7天内下单的用户的下单数量之和
    14天订单数
    28天订单数
    60天订单数
    90天订单数
    180天订单数

Drop table dwb.dwb_vova_buyer_life_cycle_monitor;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_buyer_life_cycle_monitor (
  datasource            string   COMMENT 'd_datasource',
  region_code           string   COMMENT 'd_国家/地区',
  platform              string   COMMENT 'd_平台',
  main_channel          string   COMMENT 'd_主渠道',

  activate_uv           bigint         COMMENT 'i_当天激活用户数',
  bonus_day0            decimal(16,4)  COMMENT 'i_当日激活用户中在当日下单的用户所用补贴之和',
  bonus_day0_30         decimal(16,4)  COMMENT 'i_当日激活用户中在0-30天内下单的用户所用补贴之和',
  activate_pay_day0_uv  bigint         COMMENT 'i_当天新激活下单用户数',
  activate_pay_day3_uv  bigint         COMMENT 'i_3天下单用户数',
  activate_pay_day7_uv  bigint         COMMENT 'i_7天下单用户数',
  activate_pay_day14_uv bigint         COMMENT 'i_14天下单用户数',
  activate_pay_day28_uv bigint         COMMENT 'i_28天下单用户数',
  gmv_day0              decimal(16,4)  COMMENT 'i_当天GMV',
  gmv_day0_3            decimal(16,4)  COMMENT 'i_3天GMV',
  gmv_day0_7            decimal(16,4)  COMMENT 'i_7天GMV',
  gmv_day0_14           decimal(16,4)  COMMENT 'i_14天GMV',
  gmv_day0_28           decimal(16,4)  COMMENT 'i_28天GMV',
  order_cnt_day0        bigint         COMMENT 'i_当天订单数',
  order_cnt_day0_3      bigint         COMMENT 'i_3天订单数',
  order_cnt_day0_7      bigint         COMMENT 'i_7天订单数',
  order_cnt_day0_14     bigint         COMMENT 'i_14天订单数',
  order_cnt_day0_28     bigint         COMMENT 'i_28天订单数',
  avg_bonus_day0        double         COMMENT 'i_当日补贴成本',
  avg_bonus_day0_30     double         COMMENT 'i_30天补贴成本',
  liv_day0              double         COMMENT '当天LTV',
  liv_day0_3            double         COMMENT '3天LTV',
  liv_day0_7            double         COMMENT '7天LTV',
  liv_day0_14           double         COMMENT '14天LTV',
  liv_day0_28           double         COMMENT '28天LTV',
  cr                    double         COMMENT '当天转化率',
  cr_day0_3             double         COMMENT '3天转化率',
  cr_day0_7             double         COMMENT '7天转化率',
  cr_day0_14            double         COMMENT '14天转化率',
  cr_day0_28            double         COMMENT '28天转化率',
  avg_order_cnt         double         COMMENT '当天平均订单数',
  avg_order_cnt_day0_3  double         COMMENT '3天平均订单数',
  avg_order_cnt_day0_7  double         COMMENT '7天平均订单数',
  avg_order_cnt_day0_14 double         COMMENT '14天平均订单数',
  avg_order_cnt_day0_28 double         COMMENT '28天平均订单数'
) COMMENT '用户生命周期监控报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_buyer_life_cycle_monitor/"
;

#################################################
[8661]部分字段口径修改【用户生命周期监控报表】
https://zt.gitvv.com/index.php?m=task&f=view&taskID=32798

在 [8070]用户生命周期监控报表 上修改
任务描述
1、当日补贴成本和30日补贴成本字段口径修改；
2、增加60天、90天、180天相关数据；
https://docs.google.com/spreadsheets/d/19T8u5FUpg_AimNmB_880I-3R0IgdTJgy0-CZm-JmVOg/edit#gid=2010163347

# activate_pay_day28_uv bigint         COMMENT 'i_28天下单用户数',
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(activate_pay_day60_uv int comment '60天下单用户数') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(activate_pay_day90_uv int comment '90天下单用户数') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(activate_pay_day180_uv int comment '180天下单用户数') cascade;

# gmv_day0_28           decimal(16,4)  COMMENT 'i_28天GMV',
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(gmv_day0_60 decimal(16,4) comment '60天GMV') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(gmv_day0_90 decimal(16,4) comment '90天GMV') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(gmv_day0_180 decimal(16,4) comment '180天GMV') cascade;

# order_cnt_day0_28     bigint         COMMENT 'i_28天订单数',
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(order_cnt_day0_60 int comment '60天订单数') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(order_cnt_day0_90 int comment '90天订单数') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(order_cnt_day0_180 int comment '180天订单数') cascade;

# liv_day0_28           double         COMMENT '28天LTV',
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(liv_day0_60 double comment '60天LTV') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(liv_day0_90 double comment '90天LTV') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(liv_day0_180 double comment '180天LTV') cascade;

# cr_day0_28            double         COMMENT '28天转化率',
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(cr_day0_60 double comment '60天转化率') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(cr_day0_90 double comment '90天转化率') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(cr_day0_180 double comment '180天转化率') cascade;

# avg_order_cnt_day0_28 double         COMMENT '28天平均订单数'
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(avg_order_cnt_day0_60 double comment '60天平均订单数') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(avg_order_cnt_day0_90 double comment '90天平均订单数') cascade;
alter table dwb.dwb_vova_buyer_life_cycle_monitor add columns(avg_order_cnt_day0_180 double comment '180天平均订单数') cascade;

# 2 ###################
当日补贴成本	当日激活用户中在当日下单的用户所用补贴之和/当天新激活用户数
30天补贴成本	当日激活用户中在0-30天内下单的用户所用补贴之和/当天新激活用户数




