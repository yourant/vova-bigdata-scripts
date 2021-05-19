#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期当天
if [ ! -n "$1" ];then
cur_date=`date  +%Y-%m-%d`
fi

spark-sql   --conf "spark.sql.autoBroadcastJoinThreshold=31457280"  \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.app.name=dwb_vova_ab_test_h" \
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


INSERT OVERWRITE TABLE tmp.vova_ab_expre_tmp_h
 select /*+ REPARTITION(60) */ nvl(datasource, 'NA')                                         datasource,
           nvl(hour, 'NA')                                               hour,
           nvl(platform, 'NA')                                           platform,
           nvl(os, 'NA')                                                 os,
           nvl(rec_page_code, 'NA')                                      rec_page_code,
           nvl(substr(ab_test, 0, length(ab_test) - length(split(ab_test, '_')[size(split(ab_test, '_')) - 1]) - 1),
               'NA')                                                     rec_code,
           nvl(split(ab_test, '_')[size(split(ab_test, '_')) - 1], 'NA') rec_version,
           device_id_expre,
           virtual_goods_id
    from (
             select datasource,
                    hour,
                    os_type                     platform,
                    app_version                 os,
                    case
                        when page_code in ('homepage','dynamic_activity_template') and list_type in ('/popular','/dynamic_activity') then 'rec_best_selling'
                        when page_code in ('homepage', 'product_list') and list_type = '/product_list_newarrival'
                            then 'rec_new_arrival'
                        when page_code in ('homepage', 'product_list') and
                             list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
                        when page_code in ('homepage', 'product_list') and list_type = '/product_list_sold'
                            then 'rec_sold'
                        when page_code in ('homepage', 'product_list') and
                             list_type in ('/product_list_price_asc', '/product_list_price_desc') then 'rec_price'
                        when page_code = 'flashsale' and list_type in ('/onsale', 'upcoming', '/upcoming')
                            then 'rec_flash_sale'
                        when page_code = 'product_detail' and list_type = '/detail_also_like' then 'rec_product_detail'
                        when page_code = 'search_result' and list_type in ('/search_result', '/search_result_recommend')
                            then 'rec_search_result'
                        when page_code = 'search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
                        when page_code = 'search_result' and
                             list_type in ('/search_result_price_desc', '/search_result_price_asc')
                            then 'rec_search_price'
                        when page_code = 'search_result' and list_type = '/search_result_newarrival'
                            then 'rec_search_newarrival'
                        when page_code = 'coins_rewards' and list_type = '/coins_rewards' then 'rec_coins_rewards'
                        when page_code = 'cart' and list_type = '/cart_also_like' then 'rec_cart'
                        when page_code = 'merchant_store' and list_type in ('/merchant_store', 'merchant_store')
                            then 'rec_merchant_store'
                        when page_code = 'me' and list_type = '/me_also_like' then 'rec_me'
                        when page_code = 'payment_success' and list_type = '/pay_success' then 'rec_payment_success'
                        when page_code = 'theme_activity' and list_type not like '%201912%' then 'rec_theme_activity'
                        when page_code = 'theme_activity' and list_type like '%201912%' then 'rec_push'
                        when page_code = 'homepage' and list_type = '/flash_sale_hp_entrance'
                            then 'rec_flash_sale_entrance'
						when page_code = 'search_result' and list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
                         when (page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list')
                           or (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like') then 'rec_robot'
                        else 'others' end       rec_page_code,
                    split(test_info, '&')       test_info,
                    concat(device_id, buyer_id) device_id_expre,
                    a.virtual_goods_id
             from (select datasource,os_type,app_version,page_code,list_type,test_info,device_id,hour,virtual_goods_id,buyer_id
                    from dwd.dwd_vova_log_goods_impression_arc where pt = '${cur_date}'
                    union all
                    select datasource,os_type,app_version,page_code,list_type,test_info,device_id,hour,cast(element_id as bigint) virtual_goods_id,buyer_id
                    from dwd.dwd_vova_log_impressions_arc where pt = '${cur_date}' and event_type='goods'
                  ) a
             where os_type in ('ios', 'android')
         ) t LATERAL VIEW explode(t.test_info) ab_tes as ab_test
    where ab_test like 'rec_%'
;



INSERT OVERWRITE TABLE tmp.vova_ab_expre_tmp_distinct_h
select /*+ REPARTITION(20) */
datasource,
hour,
platform,
rec_page_code,
rec_code,
rec_version,
device_id_expre,count(1) cnt
from tmp.vova_ab_expre_tmp_h
group by
datasource,
hour,
platform,
rec_page_code,
rec_code,
rec_version,
device_id_expre
;

INSERT OVERWRITE TABLE tmp.vova_ab_expre_tmp_uv_h
select /*+ REPARTITION(5) */
nvl(datasource,'all') datasource,nvl(hour,'all') hour,nvl(platform,'all') platform,nvl(rec_page_code,'all') rec_page_code,nvl(rec_code,'all') rec_code,nvl(rec_version,'all') rec_version,
count(distinct device_id_expre) expre_uv
from tmp.vova_ab_expre_tmp_distinct_h
group by cube (datasource,hour,platform,rec_page_code,rec_code,rec_version)
;



INSERT OVERWRITE TABLE tmp.vova_ab_expre_tmp_pv_h
select /*+ REPARTITION(5) */
nvl(datasource,'all') datasource,nvl(hour,'all') hour,nvl(platform,'all') platform,nvl(rec_page_code,'all') rec_page_code,nvl(rec_code,'all') rec_code,nvl(rec_version,'all') rec_version,
sum(cnt) expre_pv
from tmp.vova_ab_expre_tmp_distinct_h
group by cube (datasource,hour,platform,rec_page_code,rec_code,rec_version)
;


INSERT OVERWRITE TABLE tmp.vova_ab_clk_tmp_h
    select /*+ REPARTITION(1) */ nvl(datasource, 'NA')                                         datasource,
           nvl(hour, 'NA')                                         hour,
           nvl(platform, 'NA')                                           platform,
           nvl(os, 'NA')                                                 os,
           nvl(rec_page_code, 'NA')                                      rec_page_code,
           nvl(substr(ab_test, 0, length(ab_test) - length(split(ab_test, '_')[size(split(ab_test, '_')) - 1]) - 1),
               'NA')                                                     rec_code,
           nvl(split(ab_test, '_')[size(split(ab_test, '_')) - 1], 'NA') rec_version,
           device_id_clk
    from (
             select datasource,hour,
                    os_type                     platform,
                    app_version                 os,
                    case
                        when page_code in ('homepage','dynamic_activity_template') and list_type in ('/popular','/dynamic_activity') then 'rec_best_selling'
                        when page_code in ('homepage', 'product_list') and list_type = '/product_list_newarrival'
                            then 'rec_new_arrival'
                        when page_code in ('homepage', 'product_list') and
                             list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
                        when page_code in ('homepage', 'product_list') and list_type = '/product_list_sold'
                            then 'rec_sold'
                        when page_code in ('homepage', 'product_list') and
                             list_type in ('/product_list_price_asc', '/product_list_price_desc') then 'rec_price'
                        when page_code = 'flashsale' and list_type in ('/onsale', 'upcoming', '/upcoming')
                            then 'rec_flash_sale'
                        when page_code = 'product_detail' and list_type = '/detail_also_like' then 'rec_product_detail'
                        when page_code = 'search_result' and list_type in ('/search_result', '/search_result_recommend')
                            then 'rec_search_result'
                        when page_code = 'search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
                        when page_code = 'search_result' and
                             list_type in ('/search_result_price_desc', '/search_result_price_asc')
                            then 'rec_search_price'
                        when page_code = 'search_result' and list_type = '/search_result_newarrival'
                            then 'rec_search_newarrival'
                        when page_code = 'coins_rewards' and list_type = '/coins_rewards' then 'rec_coins_rewards'
                        when page_code = 'cart' and list_type = '/cart_also_like' then 'rec_cart'
                        when page_code = 'merchant_store' and list_type in ('/merchant_store', 'merchant_store')
                            then 'rec_merchant_store'
                        when page_code = 'me' and list_type = '/me_also_like' then 'rec_me'
                        when page_code = 'payment_success' and list_type = '/pay_success' then 'rec_payment_success'
                        when page_code = 'theme_activity' and list_type not like '%201912%' then 'rec_theme_activity'
                        when page_code = 'theme_activity' and list_type like '%201912%' then 'rec_push'
                        when page_code = 'homepage' and list_type = '/flash_sale_hp_entrance'
                            then 'rec_flash_sale_entrance'
						when page_code = 'search_result' and list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
                        when (page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list')
	                        or (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like') then 'rec_robot'
                        else 'others' end       rec_page_code,
                    split(test_info, '&')       test_info,
                    concat(device_id, buyer_id) device_id_clk
             from (select datasource,os_type,app_version,page_code,list_type,test_info,device_id,hour,virtual_goods_id, buyer_id
                    from dwd.dwd_vova_log_goods_click_arc where pt = '${cur_date}'
                    union all
                    select datasource,os_type,app_version,page_code,list_type,test_info,device_id,hour,cast(element_id as bigint) virtual_goods_id, buyer_id
                    from dwd.dwd_vova_log_click_arc where pt = '${cur_date}' and event_type='goods'
                 ) a
             where os_type in ('ios', 'android')
         ) t LATERAL VIEW explode(t.test_info) ab_tes as ab_test
    where ab_test like 'rec_%'
;


INSERT OVERWRITE TABLE tmp.vova_ab_clk_tmp_distinct_h
select /*+ REPARTITION(20) */
datasource,
hour,
platform,
rec_page_code,
rec_code,
rec_version,
device_id_clk,count(1) cnt
from tmp.vova_ab_clk_tmp_h
group by
datasource,
hour,
platform,
rec_page_code,
rec_code,
rec_version,
device_id_clk
;

INSERT OVERWRITE TABLE tmp.vova_ab_clk_tmp_uv_h
select /*+ REPARTITION(1) */ nvl(datasource, 'all')        datasource,
       nvl(hour, 'all')              hour,
       nvl(platform, 'all')          platform,
       nvl(rec_page_code, 'all')     rec_page_code,
       nvl(rec_code, 'all')          rec_code,
       nvl(rec_version, 'all')       rec_version,
       count(distinct device_id_clk) clk_uv
from tmp.vova_ab_clk_tmp_distinct_h tmp
group by cube (datasource, hour, platform, rec_page_code, rec_code, rec_version)
;

INSERT OVERWRITE TABLE tmp.vova_ab_clk_tmp_pv_h
select /*+ REPARTITION(1) */
nvl(datasource,'all') datasource,nvl(hour,'all') hour,nvl(platform,'all') platform,nvl(rec_page_code,'all') rec_page_code,nvl(rec_code,'all') rec_code,nvl(rec_version,'all') rec_version,
sum(cnt) clk_pv
from tmp.vova_ab_clk_tmp_distinct_h
group by cube (datasource,hour,platform,rec_page_code,rec_code,rec_version)
;


INSERT OVERWRITE TABLE tmp.vova_ab_cart_h

select /*+ REPARTITION(1) */ nvl(datasource, 'all')    datasource,
       nvl(platform, 'all')      platform,
       nvl(hour, 'all')      hour,
       nvl(rec_page_code, 'all') rec_page_code,
       nvl(rec_code, 'all')      rec_code,
       nvl(rec_version, 'all')   rec_version,
       count(distinct cart_id)   cart_uv
from (
    select datasource,
           platform,
           hour,
           rec_page_code,
           nvl(substr(ab_test, 0,
                      length(ab_test) - length(split(ab_test, '_')[size(split(ab_test, '_')) - 1]) - 1),
               'NA')                                                     rec_code,
           nvl(split(ab_test, '_')[size(split(ab_test, '_')) - 1], 'NA') rec_version,
           device_id cart_id
    from (
             select nvl(a.datasource, 'NALL')       datasource,
                    nvl(hour(from_unixtime(a.collector_tstamp / 1000,'yyyy-MM-dd HH:mm:ss')), 'NALL')       hour,
                    nvl(b.platform, 'NALL')         platform,
                    case
                        when pre_page_code in ('homepage','dynamic_activity_template') and pre_list_type in ('/popular','/dynamic_activity') then 'rec_best_selling'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type = '/product_list_sold' then 'rec_sold'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type in ('/product_list_price_asc', '/product_list_price_desc')
                            then 'rec_price'
                        when pre_page_code = 'flashsale' and pre_list_type in ('/onsale', 'upcoming', '/upcoming')
                            then 'rec_flash_sale'
                        when pre_page_code = 'product_detail' and pre_list_type = '/detail_also_like'
                            then 'rec_product_detail'
                        when pre_page_code = 'search_result' and
                             pre_list_type in ('/search_result', '/search_result_recommend')
                            then 'rec_search_result'
                        when pre_page_code = 'search_result' and pre_list_type = '/search_result_sold'
                            then 'rec_search_sold'
                        when pre_page_code = 'search_result' and
                             pre_list_type in ('/search_result_price_desc', '/search_result_price_asc')
                            then 'rec_search_price'
                        when pre_page_code = 'search_result' and pre_list_type = '/search_result_newarrival'
                            then 'rec_search_newarrival'
                        when pre_page_code = 'coins_rewards' and pre_list_type = '/coins_rewards'
                            then 'rec_coins_rewards'
                        when pre_page_code = 'cart' and pre_list_type = '/cart_also_like' then 'rec_cart'
                        when pre_page_code = 'merchant_store' and
                             pre_list_type in ('/merchant_store', 'merchant_store') then 'rec_merchant_store'
                        when pre_page_code = 'me' and pre_list_type = '/me_also_like' then 'rec_me'
                        when pre_page_code = 'payment_success' and pre_list_type = '/pay_success'
                            then 'rec_payment_success'
                        when pre_page_code = 'theme_activity' and pre_list_type not like '%201912%'
                            then 'rec_theme_activity'
                        when pre_page_code = 'theme_activity' and pre_list_type like '%201912%' then 'rec_push'
                        when pre_page_code = 'homepage' and pre_list_type = '/flash_sale_hp_entrance'
                            then 'rec_flash_sale_entrance'
						when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
                        when (pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list')
	                        or (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like') then 'rec_robot'
                        else 'others' end           rec_page_code,
                    split(pre_test_info, '&')       test_info,
                    concat(a.device_id, a.buyer_id) device_id
             from dwd.dwd_vova_fact_cart_cause_h a
                      left join dim.dim_vova_devices b
                                on a.device_id = b.device_id
             where a.pt = '${cur_date}'
         ) t LATERAL VIEW explode(t.test_info) ab_tes as ab_test
    where ab_test like 'rec_%'
) tmp
group by cube (datasource,hour, platform, rec_page_code, rec_code, rec_version)
;



INSERT OVERWRITE TABLE tmp.vova_ab_pay_h
select /*+ REPARTITION(1) */ nvl(datasource, 'all')    datasource,
       nvl(platform, 'all')      platform,
       nvl(hour, 'all')            hour,
       nvl(rec_page_code, 'all') rec_page_code,
       nvl(rec_code, 'all')      rec_code,
       nvl(rec_version, 'all')   rec_version,
       count(distinct pay_id)    pay_uv,
       sum(price)                gmv
from (
    select datasource,
           platform,
           hour,
           rec_page_code,
           nvl(substr(ab_test, 0,
                      length(ab_test) - length(split(ab_test, '_')[size(split(ab_test, '_')) - 1]) - 1),
               'NA')                                                     rec_code,
           nvl(split(ab_test, '_')[size(split(ab_test, '_')) - 1], 'NA') rec_version,
           shop_price * goods_number + shipping_fee price,
           device_id pay_id
    from (
             select nvl(a.datasource, 'NALL')       datasource,
                    nvl(b.platform, 'NALL')         platform,
                    nvl(hour(c.order_time), 'NALL')       hour,
                    case
                        when pre_page_code in ('homepage','dynamic_activity_template') and pre_list_type in ('/popular','/dynamic_activity') then 'rec_best_selling'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type in ('/product_list_popular', '/product_list') then 'rec_most_popular'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type = '/product_list_sold' then 'rec_sold'
                        when pre_page_code in ('homepage', 'product_list') and
                             pre_list_type in ('/product_list_price_asc', '/product_list_price_desc')
                            then 'rec_price'
                        when pre_page_code = 'flashsale' and pre_list_type in ('/onsale', 'upcoming', '/upcoming')
                            then 'rec_flash_sale'
                        when pre_page_code = 'product_detail' and pre_list_type = '/detail_also_like'
                            then 'rec_product_detail'
                        when pre_page_code = 'search_result' and
                             pre_list_type in ('/search_result', '/search_result_recommend')
                            then 'rec_search_result'
                        when pre_page_code = 'search_result' and pre_list_type = '/search_result_sold'
                            then 'rec_search_sold'
                        when pre_page_code = 'search_result' and
                             pre_list_type in ('/search_result_price_desc', '/search_result_price_asc')
                            then 'rec_search_price'
                        when pre_page_code = 'search_result' and pre_list_type = '/search_result_newarrival'
                            then 'rec_search_newarrival'
                        when pre_page_code = 'coins_rewards' and pre_list_type = '/coins_rewards'
                            then 'rec_coins_rewards'
                        when pre_page_code = 'cart' and pre_list_type = '/cart_also_like' then 'rec_cart'
                        when pre_page_code = 'merchant_store' and
                             pre_list_type in ('/merchant_store', 'merchant_store') then 'rec_merchant_store'
                        when pre_page_code = 'me' and pre_list_type = '/me_also_like' then 'rec_me'
                        when pre_page_code = 'payment_success' and pre_list_type = '/pay_success'
                            then 'rec_payment_success'
                        when pre_page_code = 'theme_activity' and pre_list_type not like '%201912%'
                            then 'rec_theme_activity'
                        when pre_page_code = 'theme_activity' and pre_list_type like '%201912%' then 'rec_push'
                        when pre_page_code = 'homepage' and pre_list_type = '/flash_sale_hp_entrance'
                            then 'rec_flash_sale_entrance'
						when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
                        when (pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list')
	                        or (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like') then 'rec_robot'
                        else 'others' end           rec_page_code,
                    split(pre_test_info, '&')       test_info,
                    concat(a.device_id, a.buyer_id) device_id,
                    c.goods_number,
                    c.shop_price,
                    c.goods_number,
                    c.shipping_fee
             from dwd.dwd_vova_fact_order_cause_h a
                      left join dim.dim_vova_devices b
                                on a.device_id = b.device_id
                      join (select
       og.rec_id  as order_goods_id,oi.order_time,
       og.goods_number,
       og.shop_price,
       og.shipping_fee
from ods_vova_vts.ods_vova_order_goods_h og
join ods_vova_vts.ods_vova_order_info_h oi on oi.order_id = og.order_id
where oi.pay_status >= 1
  and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  and oi.parent_order_id = 0) c
                           on a.order_goods_id = c.order_goods_id
             where a.pt = '${cur_date}'
         ) t LATERAL VIEW explode(t.test_info) ab_tes as ab_test
    where ab_test like 'rec_%'
) tmp
group by cube (datasource, platform, hour, rec_page_code, rec_code, rec_version)
;

insert overwrite table dwb.dwb_vova_ab_test_h PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(4) */
'${cur_date}' cur_date,
a.hour,
a.datasource,
a.platform,
'all' os,
a.rec_page_code,
a.rec_code,
a.rec_version,
a.expre_pv,e.clk_pv,concat(round(e.clk_pv * 100 / a.expre_pv,2),'%') ctr,d.expre_uv,f.clk_uv,
nvl(b.cart_uv,0) cart_uv,concat(nvl(round(b.cart_uv * 100 / d.expre_uv,2),0),'%') cart_rate,
nvl(round(c.gmv,2),0),0,nvl(c.pay_uv,0),concat(nvl(round(c.pay_uv * 100 / d.expre_uv,3),0),'%') cr,
nvl(round(c.pay_uv / d.expre_uv,6),0) impressions_cr,nvl(round(c.gmv  / d.expre_uv,6),0) gmv_cr
from tmp.vova_ab_expre_tmp_pv_h a
left join tmp.vova_ab_expre_tmp_uv_h d
on a.datasource = d.datasource
and a.platform = d.platform
and a.rec_page_code = d.rec_page_code
and a.rec_code = d.rec_code
and a.rec_version = d.rec_version
and a.hour = d.hour
left join tmp.vova_ab_clk_tmp_pv_h e
on a.datasource = e.datasource
and a.platform = e.platform
and a.rec_page_code = e.rec_page_code
and a.rec_code = e.rec_code
and a.rec_version = e.rec_version
and a.hour = e.hour
left join tmp.vova_ab_clk_tmp_uv_h f
on a.datasource = f.datasource
and a.platform = f.platform
and a.rec_page_code = f.rec_page_code
and a.rec_code = f.rec_code
and a.rec_version = f.rec_version
and a.hour = f.hour
left join tmp.vova_ab_cart_h b
on a.datasource = b.datasource
and a.platform = b.platform
and a.hour = b.hour
and a.rec_page_code = b.rec_page_code
and a.rec_code = b.rec_code
and a.rec_version = b.rec_version
left join tmp.vova_ab_pay_h c
on a.datasource = c.datasource
and a.platform = c.platform
and a.hour = c.hour
and a.rec_page_code = c.rec_page_code
and a.rec_code = c.rec_code
and a.rec_version = c.rec_version
;


"


#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi




