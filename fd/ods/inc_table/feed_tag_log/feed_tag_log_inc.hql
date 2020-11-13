CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_feed_tag_log_inc
(
    `feed_name` string COMMENT 'feed full name',
    `goods_id` int COMMENT 'real goods_id strat with 5',
    `log_date` string COMMENT 'log date when insert yyyy-MM-dd',
    `version_id` int  COMMENT 'version in a day 0 start',
    `virtual_goods_id` int COMMENT 'virtual goods_id start with 1',
    `cat_id`  int COMMENT 'category id',
    `product_type` string COMMENT 'feed product type(e.g. Apparel & Accessories > Clothing > Dresses)',
    `goods_thumb` string COMMENT 'goods thumbnail picture(308_422)',
    `ads_grouping` string COMMENT 'label value of ads_grouping',
    `adwords_labels` string COMMENT 'label value of adwords_labels',
    `custom_label_0` string COMMENT 'label value of custom_label_0',
    `custom_label_1` string COMMENT 'label value of custom_label_1',
    `custom_label_2` string COMMENT 'label value of custom_label_2',
    `custom_label_3` string COMMENT 'label value of custom_label_3',
    `custom_label_4` string COMMENT 'label value of custom_label_4',
    `last_update_time` string COMMENT 'log time when insert'
) COMMENT ''
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT OVERWRITE TABLE ods_fd_vb.ods_fd_feed_tag_log_inc PARTITION (dt='${hiveconf:dt}')
select
    feed_name,
    goods_id,
    log_date,
    version_id,
    virtual_goods_id,
    cat_id,
    product_type,
    goods_thumb,
    ads_grouping,
    adwords_labels,
    custom_label_0,
    custom_label_1,
    custom_label_2,
    custom_label_3,
    custom_label_4,
    last_update_time
from tmp.tmp_fd_feed_tag_log where dt = '${hiveconf:dt}';
