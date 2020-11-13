CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_summary_arc (
    inventory_summary_id  string comment '主键',
    status_id             string comment '仓库类型',
    facility_id           string comment '仓库id',
    container_id          string comment '容器id',
    product_id            string comment '产品id',
    stock_quantity        decimal(10,0) comment '库存量',
    available_to_reserved decimal(10,0) comment '可预定量',
    demand_quantity       decimal(10,0) COMMENT '订单需求量',
    making_quantity       decimal(10,0) COMMENT '',
    created_stamp         bigint comment '创建时间',
    last_updated_stamp    bigint comment '修改时间',
    last_updated_tx_stamp bigint comment '修改事务时间',
    created_tx_stamp      bigint comment '创建事务时间',
    comments              string comment '备注',
    currency_uom_id       string comment '量词',
    uom_id                string comment '量词',
    owner_party_id        string comment '用户组织id',
    party_id              string comment '商品组织id',
    unit_cost             decimal(10,4) comment '商品单价'
) comment ''
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_summary_arc PARTITION (dt = '${hiveconf:dt}')
select 
     inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,currency_uom_id,comments,uom_id,owner_party_id,party_id,unit_cost
from (

    select 
        dt,inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,currency_uom_id,comments,uom_id,owner_party_id,party_id,unit_cost,
        row_number () OVER (PARTITION BY inventory_summary_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                inventory_summary_id,
                status_id,
                facility_id,
                container_id,
                product_id,
                stock_quantity,
                available_to_reserved,
                demand_quantity,
                making_quantity,
                /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
                if(created_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as created_stamp,
                /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
                if(last_updated_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(last_updated_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as last_updated_stamp,
                /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
                if(last_updated_tx_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(last_updated_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as last_updated_tx_stamp,
                /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
                if(created_tx_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as created_tx_stamp,
                currency_uom_id,
                comments,
                uom_id,
                owner_party_id,
                party_id,
                unit_cost
        from tmp.tmp_fd_romeo_inventory_summary_full

        UNION

        select dt,inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,currency_uom_id,comments,uom_id,owner_party_id,party_id,unit_cost
        from (

            select  dt
                    inventory_summary_id,
                    status_id,
                    facility_id,
                    container_id,
                    product_id,
                    stock_quantity,
                    available_to_reserved,
                    demand_quantity,
                    making_quantity,
                    created_stamp,
                    last_updated_stamp,
                    last_updated_tx_stamp,
                    created_tx_stamp,
                    currency_uom_id,
                    comments,
                    uom_id,
                    owner_party_id,
                    party_id,
                    unit_cost,
                    row_number () OVER (PARTITION BY inventory_summary_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_inventory_summary_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
