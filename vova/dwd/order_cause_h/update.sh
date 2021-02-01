#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 hour" +%Y-%m-%d`
fi
pre_month=`date -d "2 month ago ${pt}" +%Y-%m-%d`
pre_pt=`date -d "1 day ago ${pt}" +%Y-%m-%d`

echo "$pre_month"

sql="
drop table if exists tmp.tmp_vova_fact_order_cause_order_goods_h;
create table tmp.tmp_vova_fact_order_cause_order_goods_h  STORED AS PARQUETFILE as
select
/*+ REPARTITION(1) */
oi.project_name  AS datasource,
og.rec_id                                          as order_goods_id,
og.order_id,
oi.user_id                                         as buyer_id,
ore.device_id,
case when ore.device_type in (0, 23, 24, 25) then 'pc'
     when ore.device_type in (21, 22, 26) then 'mob'
     when ore.device_type = 11 then 'ios'
     when ore.device_type = 12 then 'android'
     else 'unknown'
     end                                            as platform,
og.goods_id,
g.virtual_goods_id,
oi.order_time
from ods_vova_vts.ods_vova_order_goods_h og
left join ods_vova_vts.ods_vova_order_info_h oi on oi.order_id = og.order_id
left join ods_vova_vts.ods_vova_virtual_goods_h g on g.goods_id = og.goods_id
left join ods_vova_vts.ods_vova_order_relation_h ore on ore.order_id = oi.order_id
where oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
and to_date(oi.order_time) = '$pt';

drop table if exists tmp.tmp_fact_order_cause_h_glk_cause_h;
create table tmp.tmp_fact_order_cause_h_glk_cause_h  STORED AS PARQUETFILE as
select /*+ REPARTITION(10) */
       t1.datasource,
       t1.goods_id,
       t1.virtual_goods_id,
       t1.device_id,
       t1.buyer_id,
       t1.order_goods_id,
       t1.platform,
       t2.pre_page_code,
       t2.pre_list_type,
       t2.pre_list_uri,
       t2.pre_element_type,
       t2.pre_app_version,
       t2.pre_test_info,
       t2.pre_recall_pool
from (select datasource,
             virtual_goods_id,
             goods_id,
             platform,
             device_id,
             buyer_id,
             order_goods_id
      from tmp.tmp_vova_fact_order_cause_order_goods_h
      where date(order_time) = '$pt' and platform in('ios','android')
     ) t1
         left join
     (
         select datasource,
                virtual_goods_id,
                device_id,
                buyer_id,
                platform,
                dvce_created_tstamp,
                page_code as pre_page_code,
                list_type as pre_list_type,
                list_uri  as pre_list_uri,
                element_type as pre_element_type,
                app_version as pre_app_version,
                test_info as pre_test_info,
                recall_pool as pre_recall_pool
         from (
                  select datasource,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool,
                         row_number()
                              over(partition by datasource, device_id,virtual_goods_id
                              order by dvce_created_tstamp desc) as row_num
                  from
                (
                  select datasource,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type                                       platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool
                  from dwd.dwd_vova_log_goods_click_arc
                  where pt >= '$pre_pt'
                    and pt <= '$pt'
                    and os_type in('ios','android')
                    and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                  union all
                  select datasource,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type                                       platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool
                  from dwd.dwd_vova_log_click_arc
                  where pt >= '$pre_pt'
                    and pt <= '$pt'
                    and os_type in('ios','android')
                    and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                    and event_type='goods'
                  union all
                  select datasource,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type                                       platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool
                  from dwd.dwd_vova_log_goods_click
                  where pt >= '$pre_month'
                    and pt < '$pre_pt'
                    and os_type in('ios','android')
                    and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                 ) t
              ) t0
         where t0.row_num = 1
     ) t2
     on t1.device_id = t2.device_id and t1.virtual_goods_id = t2.virtual_goods_id and t1.datasource = t2.datasource;
drop table if exists tmp.tmp_fact_order_cause_h_expre_cause_h;
create table tmp.tmp_fact_order_cause_h_expre_cause_h  STORED AS PARQUETFILE as
select /*+ REPARTITION(10) */
       t1.datasource,
       t1.goods_id,
       t1.device_id,
       t1.buyer_id,
       t1.order_goods_id,
       t1.platform,
       t2.pre_page_code,
       t2.pre_list_type,
       t2.pre_list_uri,
       t2.pre_element_type,
       t2.pre_app_version,
       t2.pre_test_info,
       t2.pre_recall_pool
from (select datasource,
             goods_id,
             virtual_goods_id,
             device_id,
             buyer_id,
             order_goods_id,
             platform
      from tmp.tmp_fact_order_cause_h_glk_cause_h
      where pre_page_code is null
     ) t1
         left join
     (
         select datasource,
                virtual_goods_id,
                device_id,
                buyer_id,
                platform,
                dvce_created_tstamp,
                page_code as pre_page_code,
                list_type as pre_list_type,
                list_uri  as pre_list_uri,
                element_type  as pre_element_type,
                app_version  as pre_app_version,
                test_info  as pre_test_info,
                recall_pool  as pre_recall_pool
         from (
                  select datasource,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool,
                         row_number() over(partition by device_id,virtual_goods_id
                                      order by dvce_created_tstamp desc) as row_num
                  from
                 (
                  select datasource,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type                                            platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool
                  from dwd.dwd_vova_log_goods_impression_arc
                  where pt = '$pt'
                   and os_type in('ios','android')
                   and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                   union all
                   select datasource,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type                                            platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         test_info,
                         recall_pool
                  from dwd.dwd_vova_log_impressions_arc
                  where pt = '$pt'
                   and os_type in('ios','android')
                   and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                   and event_type='goods'
                 ) t
              ) t0
         where t0.row_num = 1
     ) t2
     on t1.device_id = t2.device_id and t1.virtual_goods_id = t2.virtual_goods_id and t1.datasource = t2.datasource;

insert overwrite table dwd.dwd_vova_fact_order_cause_h PARTITION (pt = '$pt')
select /*+ REPARTITION(1) */
       datasource,
       goods_id,
       device_id,
       buyer_id,
       order_goods_id,
       platform,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
 from
(
select
       datasource,
       goods_id,
       device_id,
       buyer_id,
       order_goods_id,
       platform,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
from tmp.tmp_fact_order_cause_h_glk_cause_h
where pre_page_code is not null
union all
select
       datasource,
       goods_id,
       device_id,
       buyer_id,
       order_goods_id,
       platform,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info,
       pre_recall_pool
from tmp.tmp_fact_order_cause_h_expre_cause_h
) t where order_goods_id is not null;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --queue important --conf "spark.app.name=vova_fact_order_cause_h" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



