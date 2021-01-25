#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期当天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql   --conf "spark.app.name=dwb_vova_flashsale_cats" --conf "spark.sql.autoBroadcastJoinThreshold=-1"  --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=120"  -e "
insert overwrite table dwb.dwb_vova_flashsale_cats PARTITION (pt = '${cur_date}')
select
'${cur_date}' cur_date,a.datasource,a.platform,a.first_cat_name, a.all_goods_expre, a.brand_goods_expre, a.no_brand_goods_expre,
b.gmv, brand_gmv, no_brand_gmv
from
(
select nvl(nvl(a.datasource,'NA'),'all') datasource,nvl(nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA'),'all') platform,nvl(nvl(regexp_replace(b.first_cat_name, '\'', '') ,'NA'),'all') first_cat_name,
count(distinct a.virtual_goods_id)                                                                 all_goods_expre,
count(distinct if(b.brand_id > 0, a.virtual_goods_id, null)) brand_goods_expre,
count(distinct if(b.brand_id <= 0,a.virtual_goods_id, null)) no_brand_goods_expre
from dwd.dwd_vova_log_goods_impression a
join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
where a.page_code in ('h5flashsale', 'flashsale', 'h5flashsale_catlist')
and a.pt = '${cur_date}' and a.platform != '' and a.platform is not null and a.os_type != '' and a.os_type is not null
group by cube (nvl(a.datasource,'NA'),nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA'),nvl(regexp_replace(b.first_cat_name, '\'', ''),'NA'))
) a
left join
(
select
nvl(nvl(a.datasource,'NA'),'all') datasource,nvl(nvl(c.platform,'NA'),'all') platform,nvl(nvl(regexp_replace(c.first_cat_name, '\'', ''),'NA'),'all') first_cat_name,
sum(c.shop_price * c.goods_number + c.shipping_fee) gmv,
sum(if(b.brand_id > 0,c.shop_price * c.goods_number + c.shipping_fee,0)) brand_gmv,
sum(if(b.brand_id <= 0,c.shop_price * c.goods_number + c.shipping_fee,0)) no_brand_gmv
from dwd.dwd_vova_fact_order_cause_v2 a
join dwd.dwd_vova_fact_pay c on a.order_goods_id = c.order_goods_id
join dim.dim_vova_goods b on a.goods_id = b.goods_id
where a.pre_page_code in ('h5flashsale','flashsale','h5flashsale_catlist','homepage') and a.pt = '${cur_date}' and to_date(c.pay_time) = '${cur_date}'
AND a.pre_list_type in ('/hp_flashsale','/hp_flashsale/','/h5flashsale_category_list','/h5flashsale_main_list','/flashsale_2','/flashsale','/onsale','/upcoming','onsale','upcoming')
and c.platform != '' and c.platform is not null
group by cube (nvl(a.datasource,'NA'),nvl(c.platform,'NA'),nvl(regexp_replace(c.first_cat_name, '\'', ''),'NA'))
) b on a.datasource =b.datasource and a.platform = b.platform and a.first_cat_name = b.first_cat_name

"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi



