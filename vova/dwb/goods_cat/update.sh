#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

echo "${cur_date}"
job_name="dwb_vova_goods_cat_rpt_req3542_chenkai_${cur_date}"

sql="
set hive.new.job.grouping.set.cardinality=64;
with tmp1 as
(
SELECT nvl(datasource, 'all')               datasource,
       nvl(region_code, 'all')              region_code,
       nvl(first_cat_name, 'all')           first_cat_name,
       nvl(second_cat_name, 'all')          second_cat_name,
       nvl(third_cat_name, 'all')           third_cat_name,
       nvl(is_brand, 'all')                 is_brand,
       nvl(rec_page_code, 'all')            rec_page_code,
       sum(impressions)                     impression_pv,
       count(DISTINCT impression_device_id) impression_uv,
       sum(clicks)                          click_pv,
       count(DISTINCT click_device_id)      click_uv
FROM (
SELECT nvl(datasource, 'NA')      datasource,
       nvl(country, 'NA')         region_code,
       virtual_goods_id,
       nvl(first_cat_name, 'NA')  first_cat_name,
       nvl(second_cat_name, 'NA') second_cat_name,
       nvl(third_cat_name, 'NA') third_cat_name,
       if(brand_id > 0, 'Y', 'N') is_brand,
       click_device_id,
       impression_device_id,
       clicks,
       impressions,
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
  when page_code = 'search_result' and list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
  else 'others' end rec_page_code
from
(
select t1.*,g.brand_id,g.first_cat_name,g.second_cat_name,g.third_cat_name from
(select datasource,'goods_impression' event_name,country,virtual_goods_id,device_id,page_code,list_type,device_id impression_device_id,'NA' click_device_id,0 clicks,1 impressions from dwd.dwd_vova_log_goods_impression where pt='${cur_date}'
union all
select datasource,'goods_click' event_name,country,virtual_goods_id,device_id,page_code,list_type,'NA' impression_device_id,device_id click_device_id,1 clicks,0 impressions from dwd.dwd_vova_log_goods_click where pt='${cur_date}'
)t1
left join dim.dim_vova_goods g on t1.virtual_goods_id=g.virtual_goods_id
)t2
)t3 group by datasource,region_code,first_cat_name,second_cat_name,third_cat_name,is_brand,rec_page_code with cube
),
tmp2 as
(
SELECT nvl(datasource, 'all')                        datasource,
       nvl(first_cat_name, 'all')                    first_cat_name,
       nvl(second_cat_name, 'all')                   second_cat_name,
       nvl(third_cat_name, 'all')                    third_cat_name,
       nvl(region_code, 'all')                       region_code,
       nvl(is_brand, 'all')                          is_brand,
       nvl(rec_page_code, 'all')                     rec_page_code,
       sum(shipping_fee + goods_number * shop_price) gmv,
       sum(shipping_fee + goods_number * shop_price) / sum(goods_number) avg_shop_price,
       count(DISTINCT order_goods_id)                paid_order_cnt,
       count(DISTINCT buyer_id)                      users
FROM (
         SELECT nvl(py.datasource, 'NA')      datasource,
                nvl(py.first_cat_name, 'NA')  first_cat_name,
                nvl(py.second_cat_name, 'NA') second_cat_name,
                nvl(g.third_cat_name, 'NA') third_cat_name,
                nvl(py.region_code, 'NA')     region_code,
                if(g.brand_id > 0, 'Y', 'N')  is_brand,
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
  when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
  else 'others' end rec_page_code,
py.order_goods_id,
py.buyer_id,py.shipping_fee,py.goods_number,py.shop_price
from  dwd.dwd_vova_fact_pay  py
inner join  dim.dim_vova_order_goods ddog on ddog.order_goods_id = py.order_goods_id
left join dim.dim_vova_goods g on g.goods_id=py.goods_id
left join (select * from dwd.dwd_vova_fact_order_cause_v2 where pre_page_code is not null and pt='${cur_date}') oc on py.order_goods_id=oc.order_goods_id
where to_date(py.pay_time)='${cur_date}' and (py.from_domain like '%api.vova%' or py.from_domain like '%api.airyclub%')
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
)t group by datasource,first_cat_name,second_cat_name,third_cat_name,region_code,is_brand,rec_page_code with cube
),
tmp3 as
(
SELECT nvl(datasource, 'all')      datasource,
       nvl(first_cat_name, 'all')  first_cat_name,
       nvl(second_cat_name, 'all') second_cat_name,
       nvl(third_cat_name, 'all')  third_cat_name,
       nvl(region_code, 'all')     region_code,
       nvl(is_brand, 'all')        is_brand,
       nvl(rec_page_code, 'all')   rec_page_code,
       count(DISTINCT device_id)   cart_success_uv
FROM (
         SELECT nvl(c.datasource, 'NA')      datasource,
                nvl(g.first_cat_name, 'NA')  first_cat_name,
                nvl(g.second_cat_name, 'NA') second_cat_name,
                nvl(g.third_cat_name, 'NA')  third_cat_name,
                nvl(c.country, 'NA')         region_code,
                if(g.brand_id > 0, 'Y', 'N') is_brand,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
  when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
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
  when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
  else 'others' end rec_page_code,
device_id
from dwd.dwd_vova_fact_cart_cause_v2 c
left join dim.dim_vova_goods g on c.virtual_goods_id = g.virtual_goods_id
where c.pt='$cur_date' and c.pre_page_code is not null
)t group by datasource,first_cat_name,second_cat_name,third_cat_name,region_code,is_brand,rec_page_code with cube
),
tmp4 as
(
select
nvl(nvl(dg.first_cat_name,'NA'),'all') AS first_cat_name,
nvl(nvl(dg.second_cat_name,'NA'),'all') AS second_cat_name,
nvl(nvl(dg.third_cat_name,'NA'),'all') AS third_cat_name,
nvl(if(dg.brand_id>0,'Y','N'), 'all') AS is_brand,
count(*) AS is_on_sale_cnt
from
dim.dim_vova_goods dg
WHERE dg.is_on_sale = 1
group by cube (nvl(dg.first_cat_name,'NA'), nvl(dg.second_cat_name,'NA'), nvl(dg.third_cat_name,'NA'), if(dg.brand_id>0,'Y','N'))
)

INSERT OVERWRITE TABLE dwb.dwb_vova_goods_gcr_gmv_report PARTITION (pt = '${cur_date}')
SELECT '${cur_date}'                                                                  event_date,
       t1.datasource,
       t1.region_code,
       t1.first_cat_name,
       t1.second_cat_name,
       t1.is_brand,
       t1.rec_page_code,
       nvl(t1.impression_pv, 0)                                                       impression_pv,
       nvl(t1.impression_uv, 0)                                                       impression_uv,
       nvl(t2.gmv, 0)                                                                 gmv,
       nvl(t1.click_pv, 0)                                                            click_pv,
       nvl(t2.users, 0)                                                               pay_uv,
       round(t2.gmv * 100 / t1.click_uv * t1.click_pv * 100 / t1.impression_pv, 2) AS gcr,
       0                                                     cart_uv,
       nvl(t3.cart_success_uv, 0)                                                     cart_success_uv,
       nvl(t1.click_pv / t1.impression_pv, 0)                                                     AS ctr,
       nvl(t2.users / t1.impression_uv, 0)                                                       AS pay_uv_div_impression_uv,
       nvl(t3.cart_success_uv / t1.impression_uv, 0)                                              AS cart_success_uv_div_impression_uv,
       t1.third_cat_name,
       nvl(t2.avg_shop_price, 0) AS avg_shop_price,
       nvl(t2.paid_order_cnt, 0) AS paid_order_cnt,
       nvl(t4.is_on_sale_cnt, 0) AS is_on_sale_cnt
FROM tmp1 t1
         LEFT JOIN tmp2 t2 ON t1.datasource = t2.datasource
         AND t1.region_code = t2.region_code
         AND t1.first_cat_name = t2.first_cat_name
         AND t1.second_cat_name = t2.second_cat_name
         AND t1.is_brand = t2.is_brand
         AND t1.rec_page_code = t2.rec_page_code
         AND t1.third_cat_name = t2.third_cat_name
         LEFT JOIN tmp3 t3 ON t1.datasource = t3.datasource
         AND t1.region_code = t3.region_code
         AND t1.first_cat_name = t3.first_cat_name
         AND t1.second_cat_name = t3.second_cat_name
         AND t1.is_brand = t3.is_brand
         AND t1.rec_page_code = t3.rec_page_code
         AND t1.third_cat_name = t3.third_cat_name
         LEFT JOIN tmp4 t4 ON t1.first_cat_name = t4.first_cat_name
         AND t1.second_cat_name = t4.second_cat_name
         AND t1.third_cat_name = t4.third_cat_name
         AND t1.is_brand = t4.is_brand
WHERE t1.region_code IN
      ('all', 'GB', 'FR', 'DE', 'IT', 'ES', 'AT', 'PL', 'NO', 'US', 'CZ', 'SK', 'RU', 'BR', 'SE', 'CN', 'CH', 'TR',
       'AU', 'TW');
"
#hive -e "$sql"
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.default.parallelism=80"  \
--conf "spark.app.name=${job_name}"  \
--conf "spark.sql.autoBroadcastJoinThreshold=52428800" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.dynamicAllocation.initialExecutors=80"  \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
-e "$sql"

if [ $? -ne 0 ]; then
  echo "物品分类gcr统计${cur_date}错误"
  exit 1
fi

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# spark-submit --master yarn --deploy-mode cluster \
# --conf spark.executor.memory=10g \
# --conf spark.dynamicAllocation.maxExecutors=20 \
# --conf spark.app.name=alarm_system \
# --conf spark.executor.memoryOverhead=2048 \
# --jars /usr/share/java/javamail.jar \
# --class com.vova.monitor.MonitorMain s3://vomkt-emr-rec/jar/rpt-monitor-1.0.2.jar \
# --env product --db rpt --tlb goods_gcr_gmv_report --op check_index,send_message \
# --date ${cur_date}
#
# #如果脚本失败，则报错
# if [ $? -ne 0 ];then
#   exit 1
# fi


