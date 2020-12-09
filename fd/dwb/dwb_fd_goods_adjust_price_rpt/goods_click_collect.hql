set hive.new.job.grouping.set.cardinality = 256;
insert overwrite table dwb.dwb_fd_mid_goods_click_collect partition (pt='${pt3}')
select /*+ REPARTITION(1) */
    nvl(tab1.virtual_goods_id,'all') as virtual_goods_id,
    nvl(tab1.project,'all') as project,
    nvl(tab1.country_code ,'all') as country_code,
    nvl(tab1.platform_type ,'all') as platform_type,
    CAST(COUNT(DISTINCT add_session_before) AS FLOAT) / CAST(COUNT(DISTINCT goods_view_session_before) AS FLOAT) as add_rate_before,
    CAST(COUNT(DISTINCT add_session_after) AS FLOAT) / CAST(COUNT(DISTINCT goods_view_session_after) AS FLOAT) as add_rate_after,
    CAST(COUNT(DISTINCT paid_order_before) AS FLOAT) / CAST(COUNT(DISTINCT goods_view_session_before) AS FLOAT) as rate_before,
    CAST(COUNT(DISTINCT paid_order_after) AS FLOAT) / CAST(COUNT(DISTINCT goods_view_session_after) AS FLOAT) as rate_after,
    CAST(COUNT(DISTINCT goods_click_session_before) AS FLOAT) / CAST(COUNT(DISTINCT goods_impression_session_before) AS FLOAT) as ctr_before,
    CAST(COUNT(DISTINCT goods_click_session_after) AS FLOAT) / CAST(COUNT(DISTINCT goods_impression_session_after) AS FLOAT) as ctr_after,
    ((CAST(COUNT(DISTINCT goods_click_session_before) AS FLOAT) / CAST(COUNT(DISTINCT goods_impression_session_before) AS FLOAT))*(CAST(COUNT(DISTINCT paid_order_before) AS FLOAT) / CAST(COUNT(DISTINCT goods_view_session_before) AS FLOAT))) as cr_before,
    ((CAST(COUNT(DISTINCT goods_click_session_after) AS FLOAT) / CAST(COUNT(DISTINCT goods_impression_session_after) AS FLOAT))*(CAST(COUNT(DISTINCT paid_order_after) AS FLOAT) / CAST(COUNT(DISTINCT goods_view_session_after) AS FLOAT))) as cr_after,
    COUNT(DISTINCT goods_click_session_before) as click_before,
    COUNT(DISTINCT goods_click_session_after) as click_after,
    COUNT(DISTINCT goods_impression_session_before) as impression_before,
    COUNT(DISTINCT goods_impression_session_after) as impression_after
from (
  select
       virtual_goods_id,
       project,
       country_code,
       platform_type,
       if(pt_date <= date_sub('${pt3}',1),add_session_id,NULL) as add_session_before,
       if(pt_date >= date_add('${pt3}',1),add_session_id,NULL) as add_session_after,
       if(pt_date <= date_sub('${pt3}',1),goods_view_session_id,NULL) as goods_view_session_before,
       if(pt_date >= date_add('${pt3}',1),goods_view_session_id,NULL) as goods_view_session_after,
       if(pt_date <= date_sub('${pt3}',1),paid_order_id,NULL) as paid_order_before,
       if(pt_date >= date_add('${pt3}',1),paid_order_id,NULL) as paid_order_after,
       if(pt_date <= date_sub('${pt3}',1),goods_click_session_id,NULL) as goods_click_session_before,
       if(pt_date >= date_add('${pt3}',1),goods_click_session_id,NULL) as goods_click_session_after,
       if(pt_date <= date_sub('${pt3}',1),goods_impression_session_id,NULL) as goods_impression_session_before,
       if(pt_date >= date_add('${pt3}',1),goods_impression_session_id,NULL) as goods_impression_session_after
  from dwd.dwd_fd_goods_click_detail
  where pt = '${pt3}'
)tab1
group BY tab1.virtual_goods_id,tab1.project,tab1.country_code,tab1.platform_type with cube;