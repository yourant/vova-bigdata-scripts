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

        select  dt
                order_inv_reserved_id,
                `version`,
                status,
                order_id,
                facility_id,
                container_id,
                party_id,
                reserved_time,        
                order_time
        from ods_fd_romeo.ods_fd_romeo_order_inv_reserved_arc where dt = '${hiveconf:dt_last}'

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
