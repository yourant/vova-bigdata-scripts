CREATE TABLE IF NOT EXISTS `dwd.dwd_fd_goods_add_info`(
  `session_id` string COMMENT 'session id',
  `virtual_goods_id` int COMMENT '虚拟商品id',
  `goods_id` int COMMENT '商品id',
  `cat_id` int COMMENT '品类id',
  `cat_name` string COMMENT '品类',
  `platform_type` string COMMENT '平台类型',
  `platform` string COMMENT '平台',
  `country` string COMMENT '国家',
  `language` string COMMENT '语言',
  `project_name` string COMMENT '组织',
  `add_session_id` string COMMENT '加车session',
  `view_session_id` string COMMENT 'view session')
COMMENT '打点数据中add和view事件生成的关于商品的中间表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");

INSERT overwrite table dwd.dwd_fd_goods_add_info PARTITION (dt='${hiveconf:dt}')
select
    sae.session_id,
    sae.virtual_goods_id,
    dg.goods_id,
    dg.cat_id,
    dg.cat_name,
    sae.platform_type,
    sae.platform,
    sae.country,
    sae.language,
    sae.project_name,
    sae.add_session_id,
    sae.view_session_id
from (
    select
        session_id,
        platform_type,
        case
            when platform = 'mob' then 'APP'
            when platform = 'web' and platform_type = 'pc_web' then 'PC'
            else 'H5' end as platform,
        event_name,
        case
            when event_name IN ('page_view', 'screen_view') and page_code = 'product' then url_virtual_goods_id
            when event_name IN ('add') then single_ecommerce_event.id
            end as virtual_goods_id,
        country,
        language,
        project as project_name,
        if(event_name IN ('add'),session_id,null) as add_session_id,
        if(event_name IN ('page_view', 'screen_view') and page_code = 'product' ,session_id,null) as view_session_id
    from ods.ods_fd_snowplow_all_event
    LATERAL VIEW OUTER explode(ecommerce_product) single_ecommerce_event_table AS single_ecommerce_event
    where dt = '${hiveconf:dt}' and event_name in ('page_view', 'screen_view', 'add')
    
) sae left join dim.dim_fd_goods dg
on sae.virtual_goods_id = dg.virtual_goods_id
where sae.virtual_goods_id is not null  and sae.virtual_goods_id != '';
