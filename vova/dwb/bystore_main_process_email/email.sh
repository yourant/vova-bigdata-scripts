#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-31 day" +%Y-%m-%d`
fi

spark-submit \
--name 'dwb_vova_bystore_main_process_email' \
--master yarn  \
--conf spark.dynamicAllocation.maxExecutors=20 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "
select
pt,
is_new,
dau,
homepage_dau,
gmv,
brand_gmv,
no_brand_gmv,
payed_order_num,
first_order_num,
payed_user_num,
pon_div_pun,
g_div_pun,
g_div_pon,
pun_div_dau
from
(
select
pt,
is_new,
dau,
homepage_dau,
gmv,
brand_gmv,
no_brand_gmv,
payed_order_num,
first_order_num,
payed_user_num,
nvl(round(payed_order_num/payed_user_num,2),0) pon_div_pun,
nvl(round(gmv/payed_user_num,2),0) g_div_pun,
nvl(round(gmv/payed_order_num,2),0) g_div_pon,
concat(nvl(round(payed_user_num/dau*100,2),0),'%') pun_div_dau,
case when is_new='all' then 1
     when is_new='new' then 2
     when is_new='2-7' then 3
     when is_new='8-30' then 4
     else 5 end rank
from dwb.dwb_vova_bystore_main_process where pt>='${cur_date}'
order by pt desc,rank
) t " \
-head "日期,激活时间,dau,首页dau,gmv,brand_gmv,非brand_gmv,支付成功订单量,首单订单量,支付成功uv,人均下单数,客单价,笔单价,整体转化率"  \
-receiver "lusun@i9i8.com,lusun@vova.com.hk,huading@vova.com.hk,sanhua@vova.com.hk" \
-title "bystore近一月主流程数据"

if [ $? -ne 0 ];then
  exit 1
fi

