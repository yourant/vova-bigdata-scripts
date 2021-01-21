#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists tmp.dwd_zq_fact_cart_cause_glk_cause;
create table tmp.dwd_zq_fact_cart_cause_glk_cause as
select /*+ REPARTITION(5) */
       t2.datasource,
       event_name,
       virtual_goods_id,
       domain_userid,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
from (
         select COALESCE(page_code, last_value(page_code, true)
                                               OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_page_code,
                COALESCE(list_type, last_value(list_type, true)
                                               OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_list_type,
                COALESCE(list_uri, last_value(list_uri, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_list_uri,
                COALESCE(element_type, last_value(element_type, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_element_type,
                COALESCE(app_version, last_value(app_version, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_app_version,
                COALESCE(test_info, last_value(test_info, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_test_info,
                COALESCE(recall_pool, last_value(recall_pool, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_recall_pool,
                datasource,
                event_name,
                domain_userid,
                virtual_goods_id,
                referrer,
                dvce_created_tstamp,
                buyer_id,
                platform,
                country
         from (
                  select log.datasource,
                         dvce_created_tstamp,
                         event_name,
                         virtual_goods_id,
                         domain_userid,
                         page_code,
                         list_type,
                         list_uri,
                         referrer,
                         buyer_id,
                         platform,
                         geo_country as country,
                         element_type,
                         app_version,
                         test_info,recall_pool
                  from dwd.dwd_vova_log_goods_click log
                    inner join dim.dim_zq_site ds on ds.datasource = log.datasource
                  where log.pt = '$cur_date'
                    and log.platform in('pc','web')
                    and log.dp = 'others'
                    and log.page_code not in ('my_order','my_favorites','recently_View','recently_view')
                  union all
                  select log.datasource,
                         dvce_created_tstamp,
                         event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         domain_userid,
                         null                       page_code,
                         null                       list_type,
                         null                       list_uri,
                         referrer,
                         buyer_id,
                         platform,
                         geo_country as             country,
                         null                       element_type,
                         null                       app_version,
                         null                       test_info,
                         null                       recall_pool
                  from dwd.dwd_vova_log_data log
                    inner join dim.dim_zq_site ds on ds.datasource = log.datasource
                  where log.pt = '$cur_date'
                    and log.platform in('pc','web')
                    and log.dp = 'others'
                    and log.element_name in ('AddToCartSuccess')
                    and log.page_code in ('product_detail')
              ) t1) t2
inner join dim.dim_zq_site se on se.datasource = t2.datasource
where t2.event_name = 'data';

drop table if exists tmp.dwd_zq_fact_cart_cause_expre_cause;
create table tmp.dwd_zq_fact_cart_cause_expre_cause as
select /*+ REPARTITION(10) */
       t2.datasource,
       event_name,
       virtual_goods_id,
       domain_userid,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
from (
         select COALESCE(page_code, last_value(page_code, true)
                                               OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_page_code,
                COALESCE(list_type, last_value(list_type, true)
                                               OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_list_type,
                COALESCE(list_uri, last_value(list_uri, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_list_uri,
                COALESCE(element_type, last_value(element_type, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_element_type,
                COALESCE(app_version, last_value(app_version, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_app_version,
                COALESCE(test_info, last_value(test_info, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_test_info,
                COALESCE(recall_pool, last_value(recall_pool, true)
                                              OVER (PARTITION BY domain_userid,virtual_goods_id,datasource ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_recall_pool,
                datasource,
                event_name,
                domain_userid,
                virtual_goods_id,
                referrer,
                buyer_id,
                platform,
                country,
                dvce_created_tstamp
         from (
                  select datasource,
                         dvce_created_tstamp,
                         event_name,
                         virtual_goods_id,
                         domain_userid,
                         buyer_id,
                         platform,
                         country,
                         referrer,
                         null page_code,
                         null list_type,
                         null list_uri,
                         null element_type,
                         null app_version,
                         null test_info,
                         null recall_pool
                  from tmp.dwd_zq_fact_cart_cause_glk_cause
                  where pre_page_code is null
                  union all
                  select log.datasource,
                         dvce_created_tstamp,
                         event_name,
                         virtual_goods_id,
                         domain_userid,
                         buyer_id,
                         platform,
                         geo_country as country,
                         referrer,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,recall_pool
                  from dwd.dwd_vova_log_goods_impression log
                    inner join dim.dim_zq_site ds on ds.datasource = log.datasource
                  where log.pt = '$cur_date'
                    and log.platform in('pc','web')
                    and log.dp = 'others'
                    and log.page_code not in ('my_order','my_favorites','recently_View','recently_view')
              ) t1
     ) t2
inner join dim.dim_zq_site se on se.datasource = t2.datasource
where t2.event_name = 'data';

insert overwrite table dwd.dwd_zq_fact_cart_cause PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(2) */
       datasource,
       event_name,
       virtual_goods_id,
       domain_userid,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
from tmp.dwd_zq_fact_cart_cause_glk_cause
where pre_page_code is not null
union all
select /*+ REPARTITION(2) */
       datasource,
       event_name,
       virtual_goods_id,
       domain_userid,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
from tmp.dwd_zq_fact_cart_cause_expre_cause;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=cart_cause_v2" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
