INSERT overwrite table ods_fd_ecshop.ods_fd_order_attribute_inc  PARTITION (pt= '${hiveconf:pt}')
select attribute_id, order_id, attr_name, attr_value
from(
    select
        o_raw.xid AS event_id
        ,o_raw.`table` AS event_table
        ,o_raw.type AS event_type
        ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
        ,cast(o_raw.ts AS BIGINT) AS event_date
        ,o_raw.attribute_id
        ,o_raw.order_id
        ,o_raw.attr_name
        ,o_raw.attr_value
        ,row_number () OVER (PARTITION BY o_raw.attribute_id ORDER BY o_raw.xid DESC) AS rank
    from pdb.fd_ecshop_order_attribute
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'attribute_id', 'order_id', 'attr_name', 'attr_value') o_raw
    AS `table`, ts, `commit`, xid, type, old, attribute_id, order_id, attr_name, attr_value
    where pt= '${hiveconf:pt}'
)inc where inc.rank=1;
