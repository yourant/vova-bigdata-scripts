CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_attribute_inc (
    attribute_id bigint COMMENT '自增id',
    order_id bigint COMMENT '订单id',
    attr_name string COMMENT '扩展名',
    attr_value string COMMENT '扩展值'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_attribute_arc (
    attribute_id bigint COMMENT '自增id',
    order_id bigint COMMENT '订单id',
    attr_name string COMMENT '扩展名',
    attr_value string COMMENT '扩展值'
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_attribute (
    attribute_id bigint COMMENT '自增id',
    order_id bigint COMMENT '订单id',
    attr_name string COMMENT '扩展名',
    attr_value string COMMENT '扩展值'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

