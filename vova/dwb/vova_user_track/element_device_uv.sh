#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
insert into table dwb.dwb_vova_element_device_uv PARTITION (pt = '${cur_date}')
select
nvl(t2.datasource,'all') datasource,
nvl(t2.geo_country,'all') ctry,
nvl(t2.os_type,'all') os_type,
nvl(t2.main_channel,'all') main_channel,
nvl(t2.is_new,'all') is_new,
count(distinct t2.element_device_id) as element_device_uv, -- 商品UV
count(distinct t2.selling_element_device_id) as selling_uv, -- 可售商品UV
count(distinct t2.flashsale_element_device_id) as flashsale_uv, -- FlashSale商品UV
count(distinct t2.add_cart_element_device_id) as add_cart_uv, -- 商品加车UV
count(distinct t2.also_like_impressions_element_device_id) as also_like_impressions_uv, -- 猜你喜欢商品曝光UV
count(distinct t2.also_like_click_element_device_id) as also_like_click_uv, -- 猜你喜欢商品点击UV
count(distinct t2.colorsize_click_element_device_id) as colorsize_click_uv, -- ColorSize点击UV
count(distinct t2.buying_element_device_id) as buying_uv, -- 商品购买发起UV
count(distinct t2.comfirm_buy_element_device_id) as comfirm_buy_uv -- 商品购买确认UV
from (
select
  nvl(t1.datasource,'NA') datasource,
  nvl(t1.geo_country,'NA') geo_country,
  nvl(t1.os_type,'NA') os_type,
  nvl(dd.main_channel,'NA') main_channel,
  CASE WHEN datediff(t1.pt,dd.activate_time)=0 THEN 'new' ELSE 'old' END is_new, -- 过滤条件
  CASE WHEN t1.event_type ='normal' and t1.event_name = 'impressions'
      and page_code = 'product_detail'
      and element_name = 'first_screen'
      THEN CONCAT(element_id, '_', t1.device_id)
      end element_device_id, -- 商品UV
  CASE WHEN t1.event_type ='normal' and t1.event_name = 'impressions'
      and page_code = 'product_detail'
      and element_name = 'first_screen'
      and extra_status = 'selling'
      THEN CONCAT(element_id, '_', t1.device_id)
      end selling_element_device_id, -- 可售商品UV
  CASE WHEN t1.event_type ='normal' and t1.event_name = 'impressions'
      and page_code = 'product_detail'
      and element_name = 'first_screen'
      and extra_flashsale != 'none'
      THEN CONCAT(element_id, '_', t1.device_id)
      end flashsale_element_device_id, -- FlashSale商品UV
  CASE WHEN t1.event_name ='data'
      and page_code = 'product_detail'
      and element_name = 'pdAddToCartSuccess'
      THEN CONCAT(element_id, '_',  t1.device_id)
      end add_cart_element_device_id, -- 商品加车UV
  CASE WHEN t1.event_type ='goods' and t1.event_name = 'impressions'
      and page_code = 'product_detail'
      and list_type = '/detail_also_like'
      and extra_current_goods != '' and extra_current_goods is not null
      THEN CONCAT(element_id, '_', extra_current_goods, '_', t1.device_id)
      end also_like_impressions_element_device_id, -- 猜你喜欢商品曝光UV
  CASE WHEN t1.event_type ='goods'  and event_name = 'click'
      and page_code = 'product_detail'
      and list_type = '/detail_also_like'
      and extra_current_goods is not null and extra_current_goods != ''
      THEN CONCAT(element_id, '_', extra_current_goods, '_', t1.device_id)
      end also_like_click_element_device_id, -- 猜你喜欢商品点击UV
  CASE WHEN t1.event_type ='normal'  and event_name = 'click'
      and page_code = 'product_detail'
      and element_name = 'row_colorsize'
      THEN CONCAT(element_id, '_', t1.device_id)
      end colorsize_click_element_device_id, -- ColorSize点击UV
  CASE WHEN t1.event_type ='normal'  and event_name = 'click'
      and page_code = 'product_detail'
      and element_name in ('buy_now_at_product_detail', 'pdAddToCartClick', 'buy_now_at_floating_flash_sale')
      THEN CONCAT(element_id, '_', t1.device_id)
      END buying_element_device_id, -- 商品购买发起UV
  CASE WHEN t1.event_type ='normal'  and event_name = 'click'
      and page_code = 'product_detail'
      and element_name in ('buy_now_at_product_options_dialog', 'add_to_cart_at_prodcut_options_dialog', 'confirm_buy_now', 'confirm_add_to_cart')
      THEN CONCAT(element_id, '_', t1.device_id)
      END comfirm_buy_element_device_id -- 商品购买确认UV
from(
  select pt,datasource,event_name,geo_country,os_type
      ,page_code,device_id
      ,element_id
      ,element_name
      ,get_json_object(extra,'$.status') extra_status
      ,get_json_object(extra,'$.flashsale') extra_flashsale
      ,get_json_object(extra,'$.current_goods') extra_current_goods
      ,list_type
      ,event_type
      from dwd.dwd_vova_log_impressions_arc
      where pt='${cur_date}' and page_code = 'product_detail' and (element_name = 'first_screen' or list_type = '/detail_also_like')
            and platform ='mob' and os_type is not null and os_type !='' and device_id is not null and element_id is not null
    union all
    select pt,datasource,event_name,geo_country,os_type
      ,page_code,device_id
      ,element_id
      ,element_name
      ,null extra_status
      ,null extra_flashsale
      ,null extra_current_goods
      ,null list_type
      ,null event_type
      from dwd.dwd_vova_log_data
      where pt='${cur_date}' and page_code = 'product_detail' and element_name='pdAddToCartSuccess'
            and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
    union all
    select pt,datasource,event_name,geo_country,os_type
      ,page_code,device_id
      ,element_id
      ,element_name
      ,get_json_object(extra,'$.status') extra_status
      ,get_json_object(extra,'$.flashsale') extra_flashsale
      ,get_json_object(extra,'$.current_goods') extra_current_goods
      ,list_type
      ,event_type
      from dwd.dwd_vova_log_click_arc
      where pt='${cur_date}' and page_code = 'product_detail'
          and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
) t1
    left join dim.dim_vova_devices dd on dd.device_id = t1.device_id and dd.datasource=t1.datasource
) t2
group by
   t2.datasource,
   t2.geo_country,
   t2.os_type,
   t2.main_channel,
   t2.is_new
  with cube
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 5G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_element_device_uv" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

