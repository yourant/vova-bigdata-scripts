CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_attribute_arc (
    attribute_id bigint COMMENT '自增id',
    order_id bigint COMMENT '订单id',
    attr_name string COMMENT '扩展名',
    attr_value string COMMENT '扩展值'
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_attribute_arc PARTITION (dt = '${hiveconf:dt}')
select 
     attribute_id, order_id, attr_name, attr_value
from (

    select 
        dt,attribute_id, order_id, attr_name, attr_value,
        row_number () OVER (PARTITION BY attribute_id ORDER BY dt DESC) AS rank
    from (

        select  dt
                attribute_id,
                order_id,
                attr_name,
                attr_value
        from ods_fd_ecshop.ods_fd_ecs_order_attribute_arc where dt = '${hiveconf:dt_last}'

        UNION

        select dt,attribute_id, order_id, attr_name, attr_value
        from (

            select  dt
                    attribute_id,
                    order_id,
                    attr_name,
                    attr_value,
                    row_number () OVER (PARTITION BY attribute_id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_order_attribute_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
