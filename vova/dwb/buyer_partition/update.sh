#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with rpt_buyer_partition_dau as (
select
t1.pt,
t1.buyer_id,
t2.reg_tag,
t2.buyer_act,
t2.trade_act
from
(select distinct pt,buyer_id from dwd.dwd_vova_log_screen_view where pt='$pt' and dp='vova' and platform='mob' and buyer_id>0) t1
left join ads.ads_vova_buyer_portrait_feature t2 on t1.buyer_id = t2.buyer_id
where t2.pt='$pt' and t2.datasource = 'vova' and t2.reg_tag is not null and t2.buyer_act is not null and t2.trade_act is not null
),
rpt_buyer_partition_login as (
select
t1.pt,
t1.buyer_id,
t2.reg_tag,
t2.buyer_act,
t2.trade_act
from
(select '$pt' pt,buyer_id from dim.dim_vova_buyers where datasource='vova' and bind_time is not null)  t1
left join ads.ads_vova_buyer_portrait_feature t2 on t1.buyer_id = t2.buyer_id
where t2.pt='$pt' and t2.datasource = 'vova'
),
--每日gmv
rpt_buyer_partition_gmv as (
select
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.gmv,
t1.payed_uv,
t2.total_gmv
from
(
select
'$pt' event_date,
nvl(f.reg_tag,'all') reg_tag,
nvl(f.buyer_act,'all') buyer_act,
nvl(f.trade_act,'all') trade_act,
sum(p.shop_price * p.goods_number + p.shipping_fee) gmv,
count(distinct p.buyer_id) payed_uv
from dwd.dwd_vova_fact_pay p
left join ads.ads_vova_buyer_portrait_feature f on p.buyer_id = f.buyer_id
where f.pt='$pt' and to_date(p.pay_time)='$pt' and platform in ('ios','android') and p.datasource = 'vova' and from_domain like '%api.vova%'
group by f.reg_tag,f.buyer_act,f.trade_act with cube
) t1 left join (select to_date(pay_time) event_date,sum(shop_price * goods_number + shipping_fee) total_gmv  from dwd.dwd_vova_fact_pay where to_date(pay_time)='$pt' and platform in ('ios','android') and datasource='vova' and from_domain like '%api.vova%' group by to_date(pay_time)) t2 on t1.event_date =t2.event_date
),
--每日加车
rpt_buyer_partition_cart as (
select
nvl(f.reg_tag,'all') reg_tag,
nvl(f.buyer_act,'all') buyer_act,
nvl(f.trade_act,'all') trade_act,
count(distinct c.buyer_id) cart_uv
from dwd.dwd_vova_log_common_click c
left join ads.ads_vova_buyer_portrait_feature f on c.buyer_id = f.buyer_id
where c.datasource = 'vova' and c.platform='mob' and f.pt='$pt' and c.pt='$pt'
and c.element_name='pdAddToCartSuccess' and c.buyer_id>0
group by f.reg_tag,f.buyer_act,f.trade_act with cube
),
--1、dau相关
rpt_buyer_partition_dau_res as (
select
'$pt' event_date,
'dau' buyer_scope,
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.cnt,
t1.total_cnt,
nvl(t2.gmv,0) gmv,
nvl(t2.payed_uv,0) payed_uv,
nvl(t2.total_gmv,0) total_gmv,
nvl(t3.cart_uv,0) cart_uv
from
(
select
t1.event_date,
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.cnt,
t2.total_cnt
from
(
select
'$pt' event_date,
nvl(reg_tag,'all') reg_tag,
nvl(buyer_act,'all') buyer_act,
nvl(trade_act,'all') trade_act,
count(*) cnt
from rpt_buyer_partition_dau
group by reg_tag,buyer_act,trade_act with cube
) t1 left join
(select pt event_date,count(*) total_cnt from rpt_buyer_partition_dau group by pt) t2 on t1.event_date= t2.event_date
) t1 left join rpt_buyer_partition_gmv t2 on t1.reg_tag = t2.reg_tag and t1.buyer_act = t2.buyer_act and t1.trade_act = t2.trade_act
left join rpt_buyer_partition_cart t3 on t1.reg_tag = t3.reg_tag and t1.buyer_act = t3.buyer_act and t1.trade_act = t3.trade_act
),
--2、用户真正绑定邮箱相关
rpt_buyer_partition_login_res as (
select
'$pt' event_date,
'is_login' buyer_scope,
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.cnt,
t1.total_cnt,
nvl(t2.gmv,0) gmv,
nvl(t2.payed_uv,0) payed_uv,
nvl(t2.total_gmv,0) total_gmv,
nvl(t3.cart_uv,0) cart_uv
from
(
select
t1.event_date,
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.cnt,
t2.total_cnt
from
(
select
'$pt' event_date,
nvl(reg_tag,'all') reg_tag,
nvl(buyer_act,'all') buyer_act,
nvl(trade_act,'all') trade_act,
count(*) cnt
from rpt_buyer_partition_login
group by reg_tag,buyer_act,trade_act with cube
) t1 left join
(select pt event_date,count(*) total_cnt from rpt_buyer_partition_login group by pt) t2 on t1.event_date= t2.event_date
) t1 left join rpt_buyer_partition_gmv t2 on t1.reg_tag = t2.reg_tag and t1.buyer_act = t2.buyer_act and t1.trade_act = t2.trade_act
left join rpt_buyer_partition_cart t3 on t1.reg_tag = t3.reg_tag and t1.buyer_act = t3.buyer_act and t1.trade_act = t3.trade_act
),
--3、全量用户
rpt_buyer_partition_all_res as (
select
'$pt' event_date,
'all_user' buyer_scope,
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.cnt,
t1.total_cnt,
nvl(t2.gmv,0) gmv,
nvl(t2.payed_uv,0) payed_uv,
nvl(t2.total_gmv,0) total_gmv,
nvl(t3.cart_uv,0) cart_uv
from
(
select
t1.reg_tag,
t1.buyer_act,
t1.trade_act,
t1.cnt,
t2.total_cnt
from
(
select
'$pt' event_date,
nvl(reg_tag,'all') reg_tag,
nvl(buyer_act,'all') buyer_act,
nvl(trade_act,'all') trade_act,
count(*) cnt
from ads.ads_vova_buyer_portrait_feature where pt='$pt' and datasource='vova'
group by reg_tag,buyer_act,trade_act with cube
) t1
left join (select pt event_date,count(*) total_cnt from ads.ads_vova_buyer_portrait_feature where pt='$pt' and datasource='vova' group by pt) t2 on t1.event_date =t2.event_date
) t1 left join rpt_buyer_partition_gmv t2 on t1.reg_tag = t2.reg_tag and t1.buyer_act = t2.buyer_act and t1.trade_act = t2.trade_act
left join rpt_buyer_partition_cart t3 on t1.reg_tag = t3.reg_tag and t1.buyer_act = t3.buyer_act and t1.trade_act = t3.trade_act
)
insert overwrite table dwb.dwb_vova_buyer_partition PARTITION (pt = '${pt}')
select /*+ REPARTITION(1) */ event_date, buyer_scope,reg_tag,buyer_act,trade_act,cnt,total_cnt,gmv,payed_uv,total_gmv,cart_uv from
(
select event_date, buyer_scope,reg_tag,buyer_act,trade_act,cnt,total_cnt,gmv,payed_uv,total_gmv,cart_uv from rpt_buyer_partition_dau_res
union all
select  event_date, buyer_scope,reg_tag,buyer_act,trade_act,cnt,total_cnt,gmv,payed_uv,total_gmv,cart_uv from rpt_buyer_partition_login_res
union all
select  event_date, buyer_scope,reg_tag,buyer_act,trade_act,cnt,total_cnt,gmv,payed_uv,total_gmv,cart_uv from rpt_buyer_partition_all_res
) t;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --queue important --conf "spark.app.name=dwb_vova_buyer_partition_zhangyin" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi