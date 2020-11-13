CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_feed_tag_log (
    `feed_name` string COMMENT 'feed full name',
    `goods_id` bigint COMMENT 'real goods_id strat with 5',
    `log_date` string COMMENT 'log date when insert yyyy-MM-dd ',
    `version_id` bigint  COMMENT ' version in a day 0 start ',
    `virtual_goods_id` bigint COMMENT 'virtual goods_id',
    `cat_id`  bigint COMMENT 'category id',
    `product_type` string COMMENT 'feed product type e.g. Apparel & Accessories > Clothing > Dresses',
    `goods_thumb` string COMMENT 'goods thumbnail picture 308_422 ',
    `ads_grouping` string COMMENT ' label value of ads_grouping',
    `adwords_labels` string COMMENT 'label value of adwords_labels',
    `custom_label_0` string COMMENT 'label value of custom_label_0',
    `custom_label_1` string COMMENT 'label value of custom_label_1',
    `custom_label_2` string COMMENT 'label value of custom_label_2',
    `custom_label_3` string COMMENT 'label value of custom_label_3',
    `custom_label_4` string COMMENT 'label value of custom_label_4',
    `last_update_time` string COMMENT 'log time when insert'
) COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_feed_tag_log
select `(dt)?+.+` from ods_fd_vb.ods_fd_feed_tag_log_arc where dt = '${hiveconf:dt}';
