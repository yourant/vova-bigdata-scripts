CREATE TABLE IF NOT EXISTS  `dim.dim_fd_currency`(
  `currency_id` int COMMENT '货币ID',
  `currency` string COMMENT '货币单位',
  `currency_symbol` string COMMENT '币种符号',
  `desc_en` string COMMENT '币种英文描述',
  `desc_cn` string COMMENT '币种中文描述',
  `disabled` int COMMENT '是否启用',
  `display_order` int COMMENT '显示排序',
  `currency_local_symbol` string COMMENT '本地币种符号',
  `continent` string COMMENT '州')
COMMENT '货币维度'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

insert overwrite table dim.dim_fd_currency
select currency_id,
       currency,
       currency_symbol,
       desc_en,
       desc_cn,
       disabled,
       display_order,
       currency_local_symbol,
       continent
from ods_fd_vb.ods_fd_currency;
