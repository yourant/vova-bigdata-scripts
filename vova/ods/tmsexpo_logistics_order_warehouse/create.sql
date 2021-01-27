create external TABLE ods_vova_ext.tmsexpo_logistics_order_warehouse_inc
(
json_str                string      COMMENT 'json_str'
) COMMENT 'tmsexpo_logistics_order_warehouse_inc' PARTITIONED BY (pt STRING,hour STRING)
LOCATION "s3://bigdata-offline/warehouse/pdb/vova/ffpostmaxwell/ffpost_data/tmsexpo_logistics_order_warehouse"
;

drop table if exists ods_vova_ext.tmsexpo_logistics_order_warehouse_arc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.tmsexpo_logistics_order_warehouse_arc
(
    id                 bigint,
    logistics_order_sn string,
    tracking_number    string,
    in_type            string,
    out_time           timestamp,
    cmd_out_time       timestamp,
    create_time        timestamp,
    warehouse_code     string,
    last_update_time   timestamp,
    in_time            timestamp,
    out_type           string,
    cmd_in_time        timestamp,
    kafka_type         string
) COMMENT 'tmsexpo_logistics_order_warehouse_arc'
    PARTITIONED BY (pt string);

drop table if exists ods_vova_ext.tmsexpo_logistics_order_warehouse;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.tmsexpo_logistics_order_warehouse
(
    id                 bigint,
    logistics_order_sn string,
    tracking_number    string,
    in_type            string,
    out_time           timestamp,
    cmd_out_time       timestamp,
    create_time        timestamp,
    warehouse_code     string,
    last_update_time   timestamp,
    in_time            timestamp,
    out_type           string,
    cmd_in_time        timestamp,
    kafka_type         string
) COMMENT 'tmsexpo_logistics_order_warehouse'
  STORED AS PARQUETFILE;