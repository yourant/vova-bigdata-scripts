INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_region_inc  PARTITION (pt= '${hiveconf:pt}')
select region_id, parent_id, region_name, region_type, region_cn_name, region_code
from(
    select
        o_raw.xid AS event_id
        ,o_raw.`table` AS event_table
        ,o_raw.type AS event_type
        ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
        ,cast(o_raw.ts AS BIGINT) AS event_date
        ,o_raw.region_id
        ,o_raw.parent_id
        ,o_raw.region_name
        ,o_raw.region_type
        ,o_raw.region_cn_name
        ,o_raw.region_code
        ,row_number () OVER (PARTITION BY o_raw.region_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_ecshop_ecs_region
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'region_id', 'parent_id', 'region_name', 'region_type', 'region_cn_name', 'region_code') o_raw
    AS `table`, ts, `commit`, xid, type, old, region_id, parent_id, region_name, region_type, region_cn_name, region_code
    where pt= '${hiveconf:pt}'
) inc where inc.rank = 1;
