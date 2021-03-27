[8931]活跃用户构成成分报表需求
任务描述
https://docs.google.com/spreadsheets/d/1aZOnRohl890-I7LbEPSZb2Klq5iSuf2uioF3Mhxo1fk/edit#gid=1081622243

需求背景及目的
1、分析每天活跃用户的成分，分别是哪个月带来的，以寻找对DAU贡献最大的季节周期、查看群组流失情况等。
2、确定对于激活30+以上天数的用户，如何进行有意义的细分，以提升其留存

需求内容
dashboard名：Vova活跃用户构成
表一：每日活跃用户构成
日期  国家  渠道  平台  用户激活月份  活跃用户数   支付用户数

表二：每月活跃用户构成
日期  国家  渠道  平台  用户激活月份  活跃用户数   支付用户数


维度说明
筛选项 枚举值
日期  默认值last month   历史数据刷至2019年1月1日
国家  all、GB、FR、DE、IT、ES、US
渠道  all、facebook、google、organic、NA、others   main_channel
平台  all、android、ios
激活月份    all/2021-3/2021-2/2021-1/2020-12/……/其他  在有限的数据范围内找不到激活月份的，统一归为‘其他’

字段说明
表名  字段名 字段业务口径（定义、公式或逻辑）    备注
表一：每日活跃用户构成 日期
    国家
    渠道
    平台
    激活月份
    活跃用户数   设备ID去重  激活月份选all时应等于DAU
    支付用户数

drop table dwb.dwb_vova_active_user_form_d;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_active_user_form_d (
  region_code             string           COMMENT '国家',
  main_channel            string           COMMENT '主渠道',
  platform                string           COMMENT '平台',
  activate_month          string           COMMENT '激活月份',
  uv                      int              COMMENT '活跃用户数',
  pay_uv                  int              COMMENT '支付用户数'
) COMMENT '每日活跃用户构成' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;

表二：每月活跃用户构成 月份  一月更新一次
    国家
    渠道
    平台
    激活月份
    活跃用户数   设备ID去重  激活月份选all时应等于MAU
    支付用户数

drop table dwb.dwb_vova_active_user_form_m;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_active_user_form_m (
  region_code             string           COMMENT '国家',
  main_channel            string           COMMENT '主渠道',
  platform                string           COMMENT '平台',
  activate_month          string           COMMENT '激活月份',
  uv                      int              COMMENT '活跃用户数',
  pay_uv                  int              COMMENT '支付用户数'
) COMMENT '每月活跃用户构成' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;