#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "start"

spark-sql   --conf "spark.sql.autoBroadcastJoinThreshold=31457280"  \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.app.name=dwb_vova_active_order_cause" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "
--活动商详页uv
drop table if exists tmp.fact_cart_cause_v2_glk_cause_5883;
create table tmp.fact_cart_cause_v2_glk_cause_5883 as
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
                buyer_id,
                platform,
                country
         from (
                  select datasource,
                         dvce_created_tstamp,
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
                         element_type element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_log_goods_click
                  where pt = '$cur_date'
                  and os_type in('ios','android')
                  and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                  union all
                  select datasource,
                         dvce_created_tstamp,
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
                  from dwd.dwd_vova_log_common_click
                  where pt = '$cur_date'
                    and os_type in('ios','android')
              ) t1) t2
where t2.event_name = 'common_click';

drop table if exists tmp.fact_cart_cause_v2_expre_cause_5883;
create table tmp.fact_cart_cause_v2_expre_cause_5883 as
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
                dvce_created_tstamp
         from (
                  select datasource,
                         dvce_created_tstamp,
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
                  from tmp.fact_cart_cause_v2_glk_cause_5883
                  where pre_page_code is null
                  union all
                  select datasource,
                         dvce_created_tstamp,
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
                         element_type element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_log_goods_impression
                  where pt = '$cur_date'
                  and os_type in('ios','android')
                  and page_code not in ('my_order','my_favorites','recently_View','recently_view')
              ) t1
     ) t2
where t2.event_name = 'common_click';


--活动商详页uv
insert overwrite table tmp.fact_cause_v2_5883 PARTITION (pt = '${cur_date}')
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
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from tmp.fact_cart_cause_v2_glk_cause_5883
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
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from tmp.fact_cart_cause_v2_expre_cause_5883;


--尝试加购uv

drop table if exists tmp.fact_cart_cause_v2_glk_cause_5883_02;
create table tmp.fact_cart_cause_v2_glk_cause_5883_02 as
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
                buyer_id,
                platform,
                country
         from (
                  select datasource,
                         dvce_created_tstamp,
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
                         element_type element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_log_goods_click
                  where pt = '$cur_date'
                  and os_type in('ios','android')
                  and page_code not in ('my_order','my_favorites','recently_View','recently_view')
                  union all
                  select datasource,
                         dvce_created_tstamp,
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
                  from dwd.dwd_vova_log_common_click
                  where pt = '$cur_date'
                    and element_name in ('pdAddToCartClick')
                    and os_type in('ios','android')
              ) t1) t2
where t2.event_name = 'common_click';

drop table if exists tmp.fact_cart_cause_v2_expre_cause_5883_02;
create table tmp.fact_cart_cause_v2_expre_cause_5883_02 as
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
                dvce_created_tstamp
         from (
                  select datasource,
                         dvce_created_tstamp,
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
                  from tmp.fact_cart_cause_v2_glk_cause_5883_02
                  where pre_page_code is null
                  union all
                  select datasource,
                         dvce_created_tstamp,
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
                         element_type element_type,
                         app_version,
                         test_info
                  from dwd.dwd_vova_log_goods_impression
                  where pt = '$cur_date'
                  and os_type in('ios','android')
                  and page_code not in ('my_order','my_favorites','recently_View','recently_view')
              ) t1
     ) t2
where t2.event_name = 'common_click';

--尝试加购uv
insert overwrite table tmp.fact_cause_v2_5883_02 PARTITION (pt = '${cur_date}')
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
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from tmp.fact_cart_cause_v2_glk_cause_5883_02
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
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version,
       pre_test_info
from tmp.fact_cart_cause_v2_expre_cause_5883_02;


--下单uv
drop table if exists tmp.fact_cart_cause_v2_glk_cause_5883_04_before;
create table tmp.fact_cart_cause_v2_glk_cause_5883_04_before as
select a.datasource,
a.dvce_created_tstamp dvce_created_tstamp,
'common_click' event_name,
a.device_id,
a.referrer,
a.buyer_id,
a.os_type,
a.geo_country,
goods_id
from dwd.dwd_vova_log_click_arc a
lateral view explode(split(get_json_object(extra, '$.goods_id'),',')) gds_id as goods_id
where a.element_name in ('checkout_place_order') and a.extra like '%goods_id%' and a.pt = '$cur_date' and a.os_type in('ios','android')
;

drop table if exists tmp.fact_cart_cause_v2_glk_cause_5883_04;
create table tmp.fact_cart_cause_v2_glk_cause_5883_04 as
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
buyer_id,
platform,
country
from (
select datasource,
dvce_created_tstamp,
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
element_type element_type,
app_version,
test_info
from dwd.dwd_vova_log_goods_click
where pt = '$cur_date'
and os_type in('ios','android')
and page_code not in ('my_order','my_favorites','recently_View','recently_view')
union all
select a.datasource,
a.dvce_created_tstamp,
a.event_name,
a.goods_id virtual_goods_id,
a.device_id,
null                       page_code,
null                       list_type,
null                       list_uri,
a.referrer,
a.buyer_id,
a.os_type as                 platform,
a.geo_country as             country,
null                       element_type,
null                       app_version,
null                       test_info
from tmp.fact_cart_cause_v2_glk_cause_5883_04_before a
) t1) t2
where t2.event_name = 'common_click';

drop table if exists tmp.fact_cart_cause_v2_expre_cause_5883_04;
create table tmp.fact_cart_cause_v2_expre_cause_5883_04 as
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
dvce_created_tstamp
from (
select datasource,
dvce_created_tstamp,
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
from tmp.fact_cart_cause_v2_glk_cause_5883_04
where pre_page_code is null
union all
select datasource,
dvce_created_tstamp,
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
element_type element_type,
app_version,
test_info
from dwd.dwd_vova_log_goods_impression
where pt = '$cur_date'
and os_type in('ios','android')
and page_code not in ('my_order','my_favorites','recently_View','recently_view')
) t1
) t2
where t2.event_name = 'common_click';

--下单uv
insert overwrite table tmp.fact_cause_v2_5883_04 PARTITION (pt = '$cur_date')
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
pre_page_code,
pre_list_type,
pre_list_uri,
pre_element_type,
pre_app_version,
pre_test_info
from tmp.fact_cart_cause_v2_glk_cause_5883_04
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
pre_page_code,
pre_list_type,
pre_list_uri,
pre_element_type,
pre_app_version,
pre_test_info
from tmp.fact_cart_cause_v2_expre_cause_5883_04;

--该活动页商详页uv

drop table if exists tmp.fact_cart_cause_v2_5883_result;
create table tmp.fact_cart_cause_v2_5883_result as
select
/*+ REPARTITION(1) */
nvl(c.datasource,'NA') datasource,
nvl(c.platform,'NA') os_type,
nvl(geo_country,'NA') country,
nvl(page_code,'NA') page_code,
nvl(element_type,'NA') element_type,
nvl(list_type,'NA') list_type,
case when page_code = 'homepage' and list_type='/popular' then 'rec_best_selling'
     when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='flashsale' and list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when page_code ='coins_rewards' and list_type ='/coins_rewards' then 'rec_coins_rewards'
     when page_code ='cart' and list_type ='/cart_also_like' then 'rec_cart'
     when page_code ='merchant_store' and list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when page_code ='me' and list_type ='/me_also_like' then 'rec_me'
     when page_code ='payment_success' and list_type ='/pay_success' then 'rec_payment_success'
     when page_code ='theme_activity' and list_type not like '%201912%' then 'rec_theme_activity'
     when page_code ='theme_activity' and list_type like '%201912%'  then 'rec_push'
     else 'others' end rec_page_code,
CASE WHEN datediff(c.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(c.pt,d.activate_time)>=1 and datediff(c.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(c.pt,d.activate_time)>=3 and datediff(c.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(c.pt,d.activate_time)>=7 and datediff(c.pt,d.activate_time)<29 THEN '8-30'
else '30+' END activate_time,
c.device_id
from dwd.dwd_vova_log_goods_click c
left join dim.dim_vova_devices d on d.device_id = c.device_id and d.datasource=c.datasource
where pt='$cur_date'
and page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01')
and geo_country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW')
;



--尝试加购uv
drop table if exists tmp.fact_cart_cause_v2_5883_02_result;
create table tmp.fact_cart_cause_v2_5883_02_result as
select
/*+ REPARTITION(1) */
nvl(c.datasource,'NA') datasource,
nvl(c.platform,'NA') os_type,
nvl(country,'NA') country,
nvl(pre_page_code,'NA') page_code,
nvl(pre_element_type,'NA') element_type,
nvl(pre_list_type,'NA') list_type,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='flashsale' and pre_list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when pre_page_code ='coins_rewards' and pre_list_type ='/coins_rewards' then 'rec_coins_rewards'
     when pre_page_code ='cart' and pre_list_type ='/cart_also_like' then 'rec_cart'
     when pre_page_code ='merchant_store' and pre_list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when pre_page_code ='me' and pre_list_type ='/me_also_like' then 'rec_me'
     when pre_page_code ='payment_success' and pre_list_type ='/pay_success' then 'rec_payment_success'
     when pre_page_code ='theme_activity' and pre_list_type not like '%201912%' then 'rec_theme_activity'
     when pre_page_code ='theme_activity' and pre_list_type like '%201912%'  then 'rec_push'
     else 'others' end rec_page_code,
CASE WHEN datediff(c.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(c.pt,d.activate_time)>=1 and datediff(c.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(c.pt,d.activate_time)>=3 and datediff(c.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(c.pt,d.activate_time)>=7 and datediff(c.pt,d.activate_time)<29 THEN '8-30'
else '30+' END activate_time,
c.device_id
from tmp.fact_cause_v2_5883_02 c
left join dim.dim_vova_devices d on d.device_id = c.device_id and d.datasource=c.datasource
where pt='$cur_date'
  and pre_page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01')
and country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW')
;


--下单uv
drop table if exists tmp.fact_order_cause_v2_5883_04_result;
create table tmp.fact_order_cause_v2_5883_04_result as
select
/*+ REPARTITION(1) */
nvl(c.datasource,'NA') datasource,
nvl(c.platform,'NA') os_type,
nvl(country,'NA') country,
nvl(pre_page_code,'NA') page_code,
nvl(pre_element_type,'NA') element_type,
nvl(pre_list_type,'NA') list_type,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='flashsale' and pre_list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when pre_page_code ='coins_rewards' and pre_list_type ='/coins_rewards' then 'rec_coins_rewards'
     when pre_page_code ='cart' and pre_list_type ='/cart_also_like' then 'rec_cart'
     when pre_page_code ='merchant_store' and pre_list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when pre_page_code ='me' and pre_list_type ='/me_also_like' then 'rec_me'
     when pre_page_code ='payment_success' and pre_list_type ='/pay_success' then 'rec_payment_success'
     when pre_page_code ='theme_activity' and pre_list_type not like '%201912%' then 'rec_theme_activity'
     when pre_page_code ='theme_activity' and pre_list_type like '%201912%'  then 'rec_push'
     else 'others' end rec_page_code,
CASE WHEN datediff(c.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(c.pt,d.activate_time)>=1 and datediff(c.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(c.pt,d.activate_time)>=3 and datediff(c.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(c.pt,d.activate_time)>=7 and datediff(c.pt,d.activate_time)<29 THEN '8-30'
else '30+' END activate_time,
c.device_id
from tmp.fact_cause_v2_5883_04 c
left join dim.dim_vova_devices d on d.device_id = c.device_id and d.datasource=c.datasource
where pt='$cur_date'
and pre_page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01')
and country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW')
;


drop table if exists tmp.fact_cart_cause_v2_5883_result_tmp;
create table tmp.fact_cart_cause_v2_5883_result_tmp as
    select
    nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as page_detail_uv
    from
    tmp.fact_cart_cause_v2_5883_result
    group by
    if(datasource in ('vova','airyclub'),datasource,'app-group'),
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    union all
    select
    nvl(datasource,'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as page_detail_uv
    from
    tmp.fact_cart_cause_v2_5883_result where datasource not in ('vova','airyclub')
    group by
    datasource,
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    having datasource != 'all'
;

drop table if exists tmp.fact_cart_cause_v2_5883_02_result_tmp;
create table tmp.fact_cart_cause_v2_5883_02_result_tmp as
    select
    nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as try_cart_uv
    from
    tmp.fact_cart_cause_v2_5883_02_result
    group by
    if(datasource in ('vova','airyclub'),datasource,'app-group'),
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    union all
    select
    nvl(datasource,'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as try_cart_uv
    from
    tmp.fact_cart_cause_v2_5883_02_result where datasource not in ('vova','airyclub')
    group by
    datasource,
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    having datasource != 'all'
;

drop table if exists tmp.rpt_rec_report_cart_cause_tmp;
create table tmp.rpt_rec_report_cart_cause_tmp as
    select
    nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as cart_uv
    from
    dwd.dwd_vova_rec_report_cart_cause
    where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01') and pt = '${cur_date}'
    group by
    if(datasource in ('vova','airyclub'),datasource,'app-group'),
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    union all
    select
    nvl(datasource,'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as cart_uv
    from
    dwd.dwd_vova_rec_report_cart_cause
    where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01') and pt = '${cur_date}'
    and datasource not in ('vova','airyclub')
    group by
    datasource,
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    having datasource != 'all'
;


drop table if exists tmp.fact_order_cause_v2_5883_04_result_tmp;
create table tmp.fact_order_cause_v2_5883_04_result_tmp as
    select
    nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as order_uv
    from
    tmp.fact_order_cause_v2_5883_04_result
    group by
    if(datasource in ('vova','airyclub'),datasource,'app-group'),
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    union all
    select
    nvl(datasource,'all') datasource,
    nvl(country,'all') country,
    nvl(os_type,'all') os_type,
    nvl(page_code,'all') page_code,
    nvl(element_type,'all') element_type,
    nvl(list_type,'all') list_type,
    nvl(activate_time,'all') activate_time,
    count(distinct device_id)  as order_uv
    from
    tmp.fact_order_cause_v2_5883_04_result where datasource not in ('vova','airyclub')
    group by
    datasource,
    country,
    os_type,
    page_code,
    element_type,
    list_type,
    activate_time
    with cube
    having datasource != 'all'
;
insert overwrite table dwb.dwb_rec_active_report_rate_analysis  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
t1.datasource,
t1.country,
t1.os_type,
t1.page_code,
t1.list_type,
t1.activate_time,
concat(round(t3.page_uv * 100 / t2.dau, 2), '%') active_page_income_rate,
concat(round(t1.page_detail_uv * 100 / t3.page_uv, 2), '%') goods_view_rate,
concat(round(t4.try_cart_uv * 100 / t1.page_detail_uv, 2), '%') try_cart_rate,
concat(round(t5.cart_uv * 100 / t1.page_detail_uv, 2), '%') cart_rate,
concat(round(t7.order_uv * 100 / t5.cart_uv, 2), '%') cart_order_rate,
0,
concat(round(t9.payed_uv* 100 / t8.order_uv, 2), '%') pay_success_rate,
concat(round(t9.payed_uv* 100 / t3.page_uv, 2), '%') total_change_rate,
t1.element_type
from
tmp.fact_cart_cause_v2_5883_result_tmp
t1
left join (
select
nvl(activate_time,'all') activate_time,nvl(datasource,'all') datasource,count(distinct dau) dau
from (
select gi.device_id          dau,
CASE
WHEN datediff(gi.pt, d.activate_time) <= 0 THEN 'new'
WHEN datediff(gi.pt, d.activate_time) >= 1 and datediff(gi.pt, d.activate_time) < 2 THEN '2-3'
WHEN datediff(gi.pt, d.activate_time) >= 3 and datediff(gi.pt, d.activate_time) < 6 THEN '4-7'
WHEN datediff(gi.pt, d.activate_time) >= 7 and datediff(gi.pt, d.activate_time) < 29 THEN '8-30'
else '30+' END activate_time,gi.datasource
from dwd.dwd_vova_log_screen_view gi
left join dim.dim_vova_devices d on d.device_id = gi.device_id and d.datasource = gi.datasource
where gi.pt = '${cur_date}'
and gi.platform = 'mob'
and gi.os_type is not null
and gi.os_type != ''
and gi.device_id is not null
) group by activate_time,datasource with cube
) t2 on t1.activate_time = t2.activate_time and t1.datasource = t2.datasource
left join (select
nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(element_type,'all') element_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id_expre)  as page_uv
from
tmp.fact_impressions_5883_page_uv
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01')
group by
if(datasource in ('vova','airyclub'),datasource,'app-group'),
country,
os_type,
page_code,
list_type,
element_type,
activate_time
with cube
union all
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(element_type,'all') element_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id_expre)  as page_uv
from
tmp.fact_impressions_5883_page_uv
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01')
and datasource not in ('vova','airyclub')
group by
datasource,
country,
os_type,
page_code,
list_type,
element_type,
activate_time
with cube
having datasource != 'all'
) t3 on t1.datasource = t3.datasource and t1.country =t3.country and t1.os_type = t3.os_type and t1.page_code = t3.page_code and t1.activate_time =t3.activate_time and t1.list_type = t3.list_type and t1.element_type = t3.element_type
left join tmp.fact_cart_cause_v2_5883_02_result_tmp t4 on t1.datasource = t4.datasource and t1.country =t4.country and t1.os_type = t4.os_type and t1.page_code = t4.page_code and t1.list_type = t4.list_type and t1.activate_time =t4.activate_time and t1.element_type = t4.element_type
left join tmp.rpt_rec_report_cart_cause_tmp t5 on t1.datasource = t5.datasource and t1.country =t5.country and t1.os_type = t5.os_type and t1.page_code = t5.page_code and t1.list_type = t5.list_type and t1.activate_time =t5.activate_time and t1.element_type = t5.element_type
left join tmp.fact_order_cause_v2_5883_04_result_tmp t7 on t1.datasource = t7.datasource and t1.country =t7.country and t1.os_type = t7.os_type and t1.page_code = t7.page_code and t1.list_type = t7.list_type and t1.activate_time =t7.activate_time and t1.element_type = t7.element_type
left join (
select
nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id)  as order_uv
from
dwd.dwd_vova_rec_report_order_cause
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01') and pt = '${cur_date}'
group by
if(datasource in ('vova','airyclub'),datasource,'app-group'),
country,
os_type,
page_code,element_type,
list_type,
activate_time
with cube
union all
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id)  as order_uv
from
dwd.dwd_vova_rec_report_order_cause
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01') and pt = '${cur_date}'
and datasource not in ('vova','airyclub')
group by
datasource,
country,
os_type,
page_code,element_type,
list_type,
activate_time
with cube
having datasource != 'all'
) t8 on t1.datasource = t8.datasource and t1.country =t8.country and t1.os_type = t8.os_type and t1.page_code = t8.page_code and t1.list_type = t8.list_type and t1.activate_time =t8.activate_time and t1.element_type = t8.element_type
left join (
select
nvl(if(datasource in ('vova','airyclub'),datasource,'app-group'),'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id) as payed_uv
from
dwd.dwd_vova_rec_report_pay_cause
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01') and pt = '${cur_date}'
group by
if(datasource in ('vova','airyclub'),datasource,'app-group'),
country,
os_type,
page_code,element_type,
list_type,
activate_time
with cube
union all
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id) as payed_uv
from
dwd.dwd_vova_rec_report_pay_cause
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01') and pt = '${cur_date}'
and datasource not in ('vova','airyclub')
group by
datasource,
country,
os_type,
page_code,element_type,
list_type,
activate_time
with cube
having datasource != 'all'
) t9 on t1.datasource = t9.datasource and t1.country =t9.country and t1.os_type = t9.os_type and t1.page_code = t9.page_code and t1.list_type = t9.list_type and t1.activate_time =t9.activate_time and t1.element_type = t9.element_type

"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi










