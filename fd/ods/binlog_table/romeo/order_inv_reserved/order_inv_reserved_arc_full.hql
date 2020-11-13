CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserved_arc (
    order_inv_reserved_id string,
    `version`             string,
    status                string,
    order_id              bigint,
    facility_id           string,
    container_id          string,
    party_id              string,
    reserved_time         bigint,
    order_time            bigint

) comment ''
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserved_arc PARTITION (dt = '${hiveconf:dt}')
select 
     order_inv_reserved_id,`version`,status,order_id,facility_id,container_id,party_id,reserved_time,order_time
from (

    select 
        dt,order_inv_reserved_id,`version`,status,order_id,facility_id,container_id,party_id,reserved_time,order_time,
        row_number () OVER (PARTITION BY order_inv_reserved_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                order_inv_reserved_id,
                `version`,
                status,
                order_id,
                facility_id,
                container_id,
                party_id,
                if(o_raw.reserved_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.reserved_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as reserved_time,
                if(o_raw.order_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.order_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as order_time
        from tmp.tmp_fd_romeo_order_inv_reserved_full

        UNION

        select dt,order_inv_reserved_id,`version`,status,order_id,facility_id,container_id,party_id,reserved_time,order_time
        from (

            select  dt
                    order_inv_reserved_id,
                    `version`,
                    status,
                    order_id,
                    facility_id,
                    container_id,
                    party_id,
                    reserved_time,        
                    order_time,
                    row_number () OVER (PARTITION BY order_inv_reserved_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_order_inv_reserved_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
