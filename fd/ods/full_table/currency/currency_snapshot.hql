CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_currency(
  `currency_id` bigint COMMENT '自增id',
  `currency` string COMMENT '币种代号，如 CNY USD EUR',
  `currency_symbol` string COMMENT '货币符号，如 $ ￥ 等',
  `desc_en` string COMMENT '币种，英文描述',
  `desc_cn` string COMMENT '币种，中文描述',
  `disabled` bigint COMMENT '0启用；1禁用',
  `display_order` bigint COMMENT '显示排序',
  `currency_local_symbol` string COMMENT '本地货币符号缩写',
  `last_update_time` string COMMENT '最后更新时间',
  `continent` string COMMENT ''
 )comment '币种'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_currency
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_currency_arc
where dt = '${hiveconf:dt}';
