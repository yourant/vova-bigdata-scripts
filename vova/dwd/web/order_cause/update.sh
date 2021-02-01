#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_month=`date -d "1 month ago ${cur_date}" +%Y-%m-%d`

echo "$pre_month"

sql="
drop table if exists tmp.tmp_dwd_vova_web_fact_order_cause_v2_glk_cause;
create table tmp.tmp_dwd_vova_web_fact_order_cause_v2_glk_cause as
select /*+ REPARTITION(10) */
       t1.datasource,
       t1.goods_id,
       t1.virtual_goods_id,
       t1.domain_userid,
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
       t2.pre_recall_pool
from (
       select dog.goods_id,
              dog.datasource,
              dg.virtual_goods_id,
              dog.platform,
              dog.device_id AS domain_userid,
              dog.buyer_id,
              dog.order_goods_id
      from dim.dim_vova_order_goods dog
               inner join dim.dim_vova_goods dg on dg.goods_id = dog.goods_id
      where date(dog.order_time) = '$cur_date'
        AND dog.datasource = 'vova'
        AND dog.from_domain not like '%api%'
     ) t1
         left join
     (
         select datasource,
                virtual_goods_id,
                domain_userid,
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
                recall_pool as pre_recall_pool
         from (
                  select log.datasource,
                         virtual_goods_id,
                         domain_userid,
                         buyer_id,
                         platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         absolute_position,
                         test_info,
                         recall_pool,
                         row_number()
                                 over(partition by log.datasource,domain_userid,virtual_goods_id
                                 order by dvce_created_tstamp desc) as row_num
                  from dwd.dwd_vova_log_goods_click log
                  where log.pt >= '$pre_month'
                    and log.pt <= '$cur_date'
                    and log.dp = 'vova'
                    and log.datasource = 'vova'
                    and log.platform in('pc','web')
              ) t0
         where t0.row_num = 1
     ) t2
     on t1.domain_userid = t2.domain_userid and t1.virtual_goods_id = t2.virtual_goods_id AND t1.datasource = t2.datasource;
drop table if exists tmp.tmp_dwd_vova_web_fact_order_cause_v2_expre_cause;
create table tmp.tmp_dwd_vova_web_fact_order_cause_v2_expre_cause as
select /*+ REPARTITION(10) */
       t1.datasource,
       t1.goods_id,
       t1.domain_userid,
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
       t2.pre_recall_pool
from (
       select datasource,
             goods_id,
             virtual_goods_id,
             domain_userid,
             buyer_id,
             order_goods_id,
             platform
      from tmp.tmp_dwd_vova_web_fact_order_cause_v2_glk_cause
      where pre_page_code is null
     ) t1
         left join
     (
         select virtual_goods_id,
                domain_userid,
                datasource,
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
                recall_pool as pre_recall_pool
         from (
                  select log.datasource,
                         virtual_goods_id,
                         domain_userid,
                         buyer_id,
                         platform,
                         dvce_created_tstamp,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version,
                         absolute_position,
                         test_info,
                         recall_pool,
                         row_number() over(partition by log.datasource,domain_userid,virtual_goods_id
                                      order by dvce_created_tstamp desc) as row_num
                  from dwd.dwd_vova_log_goods_impression log
                  where log.pt = '$cur_date'
                    and log.platform in('pc','web')
                    and log.dp = 'vova'
                    and log.datasource = 'vova'
              ) t0
         where t0.row_num = 1
     ) t2
     on t1.domain_userid = t2.domain_userid and t1.virtual_goods_id = t2.virtual_goods_id AND t1.datasource = t2.datasource;
insert overwrite table dwd.dwd_vova_web_fact_order_cause PARTITION (pt = '$cur_date')
select /*+ REPARTITION(1) */
       datasource,
       goods_id,
       domain_userid,
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
       pre_recall_pool
from
(
select
       datasource,
       goods_id,
       domain_userid,
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
       pre_recall_pool
from tmp.tmp_dwd_vova_web_fact_order_cause_v2_glk_cause
where pre_page_code is not null
union all
select
       datasource,
       goods_id,
       domain_userid,
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
       pre_recall_pool
from tmp.tmp_dwd_vova_web_fact_order_cause_v2_expre_cause
) t where order_goods_id is not null;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwd_vova_web_fact_order_cause" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

