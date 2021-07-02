insert overwrite table ads.ads_fd_base_snowplow partition(pt = '${pt}')
select
    /*+ REPARTITION(1) */
    event_fingerprint,
    derived_ts as dt,
    country,
    domain_userid,
    useragent,

    data_event_struct.element_name,
    get_json_object(data_event_struct.extra,'$.adgroup_id'),
    get_json_object(data_event_struct.extra,'$.ads_type'),
    get_json_object(data_event_struct.extra,'$.campaign_id'),
    get_json_object(data_event_struct.extra,'$.adset_id')
from
    ods_fd_snowplow.ods_fd_snowplow_all_event
where
    pt = '${pt}'
    and event_name = 'data'