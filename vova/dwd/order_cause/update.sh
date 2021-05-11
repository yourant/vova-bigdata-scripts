#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_month=`date -d "2 month ago ${cur_date}" +%Y-%m-%d`

echo "$pre_month"

sql="
drop table if exists tmp.tmp_vova_fact_order_cause_v2_glk_cause;
create table tmp.tmp_vova_fact_order_cause_v2_glk_cause STORED AS PARQUETFILE as
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
       t2.pre_position,
       t2.pre_test_info,
       t2.pre_recall_pool,
       t2.pre_language
from (select og.datasource,
             g.virtual_goods_id,
             g.goods_id,
             og.platform,
             og.device_id,
             og.buyer_id,
             og.order_goods_id
      from dim.dim_vova_order_goods og
               left join dim.dim_vova_goods g on g.goods_id = og.goods_id
      where date(og.order_time) = '$cur_date' and og.platform in('ios','android')
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
                absolute_position as pre_position,
                test_info as pre_test_info,
                recall_pool as pre_recall_pool,
                language as pre_language
         from (
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
                         absolute_position,
                         test_info,
                         recall_pool,
                         language,
                         row_number()
                                 over(partition by datasource,device_id,virtual_goods_id
                                 order by dvce_created_tstamp desc) as row_num
                  from dwd.dwd_vova_log_goods_click
                  where pt >= '$pre_month'
                    and pt <= '$cur_date'
                    and os_type in('ios','android')
                    and page_code not in ('recently_View','recently_view')
                    and !(page_code ='my_order' and list_type ='/order_detail') and !(page_code ='my_favorites' and list_type ='/favorites')
              ) t0
         where t0.row_num = 1
     ) t2
     on t1.device_id = t2.device_id and t1.virtual_goods_id = t2.virtual_goods_id and t1.datasource = t2.datasource;
drop table if exists tmp.tmp_vova_fact_order_cause_v2_expre_cause;
create table tmp.tmp_vova_fact_order_cause_v2_expre_cause STORED AS PARQUETFILE as
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
       t2.pre_position,
       t2.pre_test_info,
       t2.pre_recall_pool,
       t2.pre_language
from (select datasource,
             goods_id,
             virtual_goods_id,
             device_id,
             buyer_id,
             order_goods_id,
             platform
      from tmp.tmp_vova_fact_order_cause_v2_glk_cause
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
                absolute_position as pre_position,
                test_info as pre_test_info,
                recall_pool as pre_recall_pool,
                language as pre_language
         from (
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
                         absolute_position,
                         test_info,
                         recall_pool,
                         language,
                         row_number() over(partition by datasource,device_id,virtual_goods_id
                                      order by dvce_created_tstamp desc) as row_num
                  from dwd.dwd_vova_log_goods_impression
                  where pt = '$cur_date'
                   and os_type in('ios','android')
                    and page_code not in ('recently_View','recently_view')
                    and !(page_code ='my_order' and list_type ='/order_detail') and !(page_code ='my_favorites' and list_type ='/favorites')
              ) t0
         where t0.row_num = 1
     ) t2
     on t1.device_id = t2.device_id and t1.virtual_goods_id = t2.virtual_goods_id and t1.datasource = t2.datasource;
insert overwrite table dwd.dwd_vova_fact_order_cause_v2 PARTITION (pt = '$cur_date')
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
       pre_position,
       pre_test_info,
       pre_recall_pool,
       pre_language
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
       pre_position,
       pre_test_info,
       pre_recall_pool,
       pre_language
from tmp.tmp_vova_fact_order_cause_v2_glk_cause
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
       pre_position,
       pre_test_info,
       pre_recall_pool,
       pre_language
from tmp.tmp_vova_fact_order_cause_v2_expre_cause
) t where order_goods_id is not null;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=dwd_vova_fact_order_cause_v2"  --conf "spark.dynamicAllocation.maxExecutors=200"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


