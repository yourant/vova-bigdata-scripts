set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_inventory_summary_inc  PARTITION (pt= '${hiveconf:pt}')
select inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
       demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,currency_uom_id,comments,uom_id,owner_party_id,party_id,unit_cost
from(
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
           o_raw.created_stamp as created_stamp,
           o_raw.last_updated_stamp as last_updated_stamp,
           o_raw.last_updated_tx_stamp as last_updated_tx_stamp,
           o_raw.created_tx_stamp as created_tx_stamp,
           o_raw.comments,
           o_raw.currency_uom_id,
           o_raw.uom_id,
           o_raw.owner_party_id,
           o_raw.party_id,
           o_raw.unit_cost,
           row_number () OVER (PARTITION BY o_raw.inventory_summary_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_romeo_inventory_summary
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'inventory_summary_id','status_id','facility_id','container_id','product_id','stock_quantity','available_to_reserved',
    'demand_quantity','making_quantity','created_stamp','last_updated_stamp','last_updated_tx_stamp','created_tx_stamp','comments','currency_uom_id','uom_id','owner_party_id','party_id','unit_cost') o_raw
    AS `table`, ts, `commit`, xid, type, old,inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
    demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,comments,currency_uom_id,uom_id,owner_party_id,party_id,unit_cost
    where pt= '${hiveconf:pt}'
)inc where inc.rank = 1;
