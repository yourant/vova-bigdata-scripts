CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_summary_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    inventory_summary_id  string comment '主键',
    status_id             string comment '仓库类型',
    facility_id           string comment '仓库id',
    container_id          string comment '容器id',
    product_id            string comment '产品id',
    stock_quantity        decimal(15, 4) comment '库存量',
    available_to_reserved decimal(15, 4) comment '可预定量',
    demand_quantity       decimal(15, 4) COMMENT '订单需求量',
    making_quantity       decimal(15, 4) COMMENT '',
    created_stamp         bigint comment '创建时间',
    last_updated_stamp    bigint comment '修改时间',
    last_updated_tx_stamp bigint comment '修改事务时间',
    created_tx_stamp      bigint comment '创建事务时间',
    comments              string comment '备注',
    currency_uom_id       string comment '量词',
    uom_id                string comment '量词',
    owner_party_id        string comment '用户组织id',
    party_id              string comment '商品组织id',
    unit_cost             decimal(15, 4) comment '商品单价'
) comment ''
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_summary_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
       o_raw.xid AS event_id,
       o_raw.`table` AS event_table,
       o_raw.type AS event_type,
       cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
       cast(o_raw.ts AS BIGINT) AS event_date,
       o_raw.inventory_summary_id,
       o_raw.status_id,
       o_raw.facility_id,
       o_raw.container_id,
       o_raw.product_id,
       o_raw.stock_quantity,
       o_raw.available_to_reserved,
       o_raw.demand_quantity,
       o_raw.making_quantity,
       /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
       if(o_raw.created_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as created_stamp,
       /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
       if(o_raw.last_updated_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.last_updated_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as last_updated_stamp,
       /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
       if(o_raw.last_updated_tx_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.last_updated_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as last_updated_tx_stamp,
       /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
       if(created_tx_stamp != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as created_tx_stamp,
       o_raw.comments,
       o_raw.currency_uom_id,
       o_raw.uom_id,
       o_raw.owner_party_id,
       o_raw.party_id,
       o_raw.unit_cost,
       hour as hour
from tmp.tmp_fd_romeo_inventory_summary
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'inventory_summary_id','status_id','facility_id','container_id','product_id','stock_quantity','available_to_reserved',
'demand_quantity','making_quantity','created_stamp','last_updated_stamp','last_updated_tx_stamp','created_tx_stamp','comments','currency_uom_id','uom_id','owner_party_id','party_id','unit_cost') o_raw
AS `table`, ts, `commit`, xid, type, old,inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,comments,currency_uom_id,uom_id,owner_party_id,party_id,unit_cost
where dt = '${hiveconf:dt}';
