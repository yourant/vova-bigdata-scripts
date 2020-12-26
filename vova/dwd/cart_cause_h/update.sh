#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=`date -d "-1 hour" "+%Y-%m-%d"`
fi
echo "$pt"

sql="
drop table if exists tmp.tmp_vova_fact_cart_cause_h_glk_cause;
create table tmp.tmp_vova_fact_cart_cause_h_glk_cause as
select /*+ REPARTITION(5) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       collector_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from (
         select COALESCE(page_code, last_value(page_code, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_page_code,
                COALESCE(list_type, last_value(list_type, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_list_type,
                COALESCE(list_uri, last_value(list_uri, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_list_uri,
                COALESCE(element_type, last_value(element_type, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_element_type,
                COALESCE(app_version, last_value(app_version, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_app_version,
                COALESCE(test_info, last_value(test_info, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_test_info,
                datasource,
                event_name,
                device_id,
                virtual_goods_id,
                referrer,
                dvce_created_tstamp,
                collector_tstamp,
                buyer_id,
                platform,
                country
         from (
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         event_name,
                         virtual_goods_id,
                         device_id,
                         page_code,
                         list_type,
                         list_uri,
                         referrer,
                         buyer_id,
                         os_type as platform,
                         geo_country as country,
                         element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_fact_log_goods_click_arc
                  where pt = '$pt'
                  and os_type in('ios','android')
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         page_code,
                         list_type,
                         list_uri,
                         referrer,
                         buyer_id,
                         os_type as platform,
                         geo_country as country,
                         element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_fact_log_click_arc
                  where pt = '$pt' and event_type ='goods'
                  and os_type in('ios','android')
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         null                       page_code,
                         null                       list_type,
                         null                       list_uri,
                         referrer,
                         buyer_id,
                         os_type as                 platform,
                         geo_country as             country,
                         null                       element_type,
                         null                       app_version,
                         null                       test_info
                  from dwd.dwd_vova_fact_log_common_click_arc
                  where pt = '$pt'
                    and element_name in ('pdAddToCartSuccess')
                    and os_type in('ios','android')
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         'common_click'             event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         null                       page_code,
                         null                       list_type,
                         null                       list_uri,
                         referrer,
                         buyer_id,
                         os_type as                 platform,
                         geo_country as             country,
                         null                       element_type,
                         null                       app_version,
                         null                       test_info
                  from dwd.dwd_vova_fact_log_click_arc
                  where pt = '$pt'
                    and element_name in ('pdAddToCartSuccess')
                    and event_type='normal'
                    and os_type in('ios','android')
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         'common_click'             event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         null                       page_code,
                         null                       list_type,
                         null                       list_uri,
                         referrer,
                         buyer_id,
                         os_type as                 platform,
                         geo_country as             country,
                         null                       element_type,
                         null                       app_version,
                         null                       test_info
                  from dwd.dwd_vova_fact_log_data_arc
                  where pt = '$pt'
                    and element_name in ('pdAddToCartSuccess')
                    and os_type in('ios','android')
              ) t1) t2
where t2.event_name = 'common_click';

drop table if exists tmp.tmp_vova_fact_cart_cause_h_expre_cause;
create table tmp.tmp_vova_fact_cart_cause_h_expre_cause as
select /*+ REPARTITION(10) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       collector_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from (
         select COALESCE(page_code, last_value(page_code, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_page_code,
                COALESCE(list_type, last_value(list_type, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_list_type,
                COALESCE(list_uri, last_value(list_uri, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_list_uri,
                COALESCE(element_type, last_value(element_type, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_element_type,
                COALESCE(app_version, last_value(app_version, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_app_version,
                COALESCE(test_info, last_value(test_info, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_test_info,
                datasource,
                event_name,
                device_id,
                virtual_goods_id,
                referrer,
                buyer_id,
                platform,
                country,
                dvce_created_tstamp,
                collector_tstamp
         from (
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         event_name,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         platform,
                         country,
                         referrer,
                         null page_code,
                         null list_type,
                         null list_uri,
                         null element_type,
                         null app_version,
                         null test_info
                  from tmp.tmp_vova_fact_cart_cause_h_glk_cause
                  where pre_page_code is null
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         event_name,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type as platform,
                         geo_country as country,
                         referrer,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_fact_log_goods_impression_arc
                  where pt = '$pt'
                  and os_type in('ios','android')
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         collector_tstamp,
                         event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type as platform,
                         geo_country as country,
                         referrer,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_fact_log_impressions_arc
                  where pt = '$pt'
                  and os_type in('ios','android')
                  and event_type='goods'
              ) t1
     ) t2
where t2.event_name = 'common_click';

insert overwrite table dwd.dwd_vova_fact_cart_cause_h PARTITION (pt = '${pt}')
select /*+ REPARTITION(2) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       collector_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from tmp.tmp_vova_fact_cart_cause_h_glk_cause
where pre_page_code is not null
union all
select /*+ REPARTITION(2) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       collector_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from tmp.tmp_vova_fact_cart_cause_h_expre_cause;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --queue important --conf "spark.app.name=vova_fact_cart_cause_h" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi