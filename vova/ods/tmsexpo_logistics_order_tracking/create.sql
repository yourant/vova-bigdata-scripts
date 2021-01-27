create external TABLE ods_vova_ext.tmsexpo_logistics_order_tracking_inc
(
json_str                string      COMMENT 'json_str'
) COMMENT 'tmsexpo_logistics_order_tracking' PARTITIONED BY (pt STRING,hour STRING)
LOCATION "s3://bigdata-offline/warehouse/pdb/vova/ffpostmaxwell/ffpost_data/tmsexpo_logistics_order_tracking"
;

drop table if exists ods_vova_ext.tmsexpo_logistics_order_tracking_arc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.tmsexpo_logistics_order_tracking_arc
(
    id                           string,
    tracking_number              string,
    carrier_id                   bigint,
    logistics_order_sn           string,
    depart_airport_failed_time   timestamp,
    depart_first_mile_time       timestamp,
    delivery_failed_time         timestamp,
    warehouse_code               string,
    arrive_dest_airport_time     timestamp,
    tracking_status              string,
    transfer_last_mile_time      timestamp,
    depart_dest_customs_time     timestamp,
    pick_up_failed_time          timestamp,
    origin_country_code          string,
    active_node_time             timestamp,
    first_obtain_time            timestamp,
    pick_up_time                 string,
    estimated_delivery_date      timestamp,
    linehaul_pick_up_failed_time timestamp,
    epc_sign_time                timestamp,
    destination_country_code     string,
    depart_epc_wh_time           timestamp,
    shipping_time                timestamp,
    arrive_epc_wh_time           timestamp,
    create_time                  timestamp,
    linehaul_pick_up_time        timestamp,
    arrive_last_mile_time        timestamp,
    tracking_tag                 string,
    last_update_time             timestamp,
    dest_customs_failure_time    timestamp,
    delivered_time               timestamp,
    arrive_first_mile_time       timestamp,
    platform_id                  string,
    data_type                    string,
    depart_airport_time          timestamp,
    shipping_label_time          timestamp,
    attempt_deliver_time         timestamp,
    kafka_type                   string
) COMMENT 'tmsexpo_logistics_order_tracking_arc'
    PARTITIONED BY (pt string);



drop table if exists ods_vova_ext.tmsexpo_logistics_order_tracking;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.tmsexpo_logistics_order_tracking
(
    id                           string,
    tracking_number              string,
    carrier_id                   bigint,
    logistics_order_sn           string,
    depart_airport_failed_time   timestamp,
    depart_first_mile_time       timestamp,
    delivery_failed_time         timestamp,
    warehouse_code               string,
    arrive_dest_airport_time     timestamp,
    tracking_status              string,
    transfer_last_mile_time      timestamp,
    depart_dest_customs_time     timestamp,
    pick_up_failed_time          timestamp,
    origin_country_code          string,
    active_node_time             timestamp,
    first_obtain_time            timestamp,
    pick_up_time                 string,
    estimated_delivery_date      timestamp,
    linehaul_pick_up_failed_time timestamp,
    epc_sign_time                timestamp,
    destination_country_code     string,
    depart_epc_wh_time           timestamp,
    shipping_time                timestamp,
    arrive_epc_wh_time           timestamp,
    create_time                  timestamp,
    linehaul_pick_up_time        timestamp,
    arrive_last_mile_time        timestamp,
    tracking_tag                 string,
    last_update_time             timestamp,
    dest_customs_failure_time    timestamp,
    delivered_time               timestamp,
    arrive_first_mile_time       timestamp,
    platform_id                  bigint,
    data_type                    bigint,
    depart_airport_time          timestamp,
    shipping_label_time          timestamp,
    attempt_deliver_time         timestamp,
    kafka_type                   string
) COMMENT 'tmsexpo_logistics_order_tracking'
   STORED AS PARQUETFILE;