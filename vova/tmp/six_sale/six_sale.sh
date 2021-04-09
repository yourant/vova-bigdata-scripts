#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "7 day ago ${cur_date}" +%Y-%m-%d`
sql="
with mct_tmp as(
select mct_id, first_cat_id from ads.ads_vova_mct_rank where pt='2021-03-30' and
(
(mct_id =4693	and first_cat_id = 5715) or
(mct_id =39806	and first_cat_id = 194 ) or
(mct_id =10431	and first_cat_id = 5715) or
(mct_id =35574	and first_cat_id = 5715) or
(mct_id =27454	and first_cat_id = 194 ) or
(mct_id =11227	and first_cat_id = 194 ) or
(mct_id =26920	and first_cat_id = 5715) or
(mct_id =20919	and first_cat_id = 5715) or
(mct_id =10141	and first_cat_id = 194 ) or
(mct_id =12294	and first_cat_id = 194 ) or
(mct_id =11802	and first_cat_id = 194 ) or
(mct_id =10339	and first_cat_id = 5715) or
(mct_id =19563	and first_cat_id = 194 )
)
),
impre as (
select
gi.pt,
case when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
else 'others' end rec_page_code,
get_rp_name(recall_pool) recall_pool,
g.mct_name,
g.first_cat_name,
gi.device_id
from dwd.dwd_vova_log_goods_impression gi
join dim.dim_vova_goods g on gi.virtual_goods_id = g.virtual_goods_id
join mct_tmp t on g.mct_id = t.mct_id and g.first_cat_id = t.first_cat_id
where pt='$cur_date' and gi.dp ='vova' and gi.platform='mob'
),
click as (
select
gi.pt,
case when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
else 'others' end rec_page_code,
get_rp_name(recall_pool) recall_pool,
g.mct_name,
g.first_cat_name,
gi.device_id
from dwd.dwd_vova_log_goods_click gi
join dim.dim_vova_goods g on gi.virtual_goods_id = g.virtual_goods_id
join mct_tmp t on g.mct_id = t.mct_id and g.first_cat_id = t.first_cat_id
where pt='$cur_date' and gi.dp ='vova' and gi.platform='mob'
),

impre_tmp as (
select
pt,
rec_page_code,
mct_name,
first_cat_name,
count(*) impre,
count(distinct device_id) impre_uv
from impre group by pt,rec_page_code,mct_name,first_cat_name
),

impre_recall_tmp as(
select
pt,
rec_page_code,
mct_name,
first_cat_name,
count(*) impre,
count(distinct device_id) impre_uv
from impre where recall_pool like '%59%' group by pt,rec_page_code,mct_name,first_cat_name
),

click_tmp as
(
select
pt,
rec_page_code,
mct_name,
first_cat_name,
count(*) click,
count(distinct device_id) click_uv
from click group by pt,rec_page_code,mct_name,first_cat_name
),
click_recall_tmp as
(
select
pt,
rec_page_code,
mct_name,
first_cat_name,
count(*) click,
count(distinct device_id) click_uv
from click where recall_pool like '%59%' group by pt,rec_page_code,mct_name,first_cat_name
),
pay_detail_tmp as (
select
'$cur_date' pt,
case when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
else 'others' end rec_page_code,
get_rp_name(pre_recall_pool) recall_pool,
dg.mct_name,
dg.first_cat_name,
og.device_id,
og.buyer_id,
og.order_goods_id,
og.goods_number,
og.goods_number * og.shop_price + og.shipping_fee as gmv
from dwd.dwd_vova_fact_pay og
join mct_tmp t on og.mct_id = t.mct_id and og.first_cat_id = t.first_cat_id
LEFT JOIN dim.dim_vova_goods dg ON og.goods_id = dg.goods_id
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
left join dim.dim_vova_devices d on d.device_id = og.device_id and d.datasource=og.datasource
where date(og.pay_time) ='$cur_date' and (oc.pt>='$pre_week' and oc.pt<='$cur_date') and d.datasource='vova'
and og.from_domain like '%api.vova%'
and oc.pre_page_code is not null
),
pay_tmp as (
select
pt,
rec_page_code,
mct_name,
first_cat_name,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv
from pay_detail_tmp group by pt,rec_page_code,mct_name,first_cat_name
),

pay_recall_tmp as (
select
pt,
rec_page_code,
mct_name,
first_cat_name,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv
from pay_detail_tmp  where recall_pool like '%59%' group by pt,rec_page_code,mct_name,first_cat_name
)
insert overwrite table tmp.tmp_vova_six_sale_d partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
im.mct_name,
im.first_cat_name,
im.rec_page_code,
im.impre_uv,
im.impre,
nvl(c.click,0) click,
nvl(c.click_uv,0) click_uv,
nvl(p.payed_number,0) payed_number,
nvl(p.payed_uv,0) payed_uv,
nvl(p.gmv,0) gmv,
nvl(ir.impre,0) recall_impre,
nvl(cr.click,0) recall_click,
nvl(pr.payed_number,0) recall_payed_number,
nvl(pr.payed_uv,0) recall_payed_uv,
nvl(pr.gmv,0) recall_gmv
from impre_tmp im
left join impre_recall_tmp ir on im.pt=ir.pt and im.rec_page_code = ir.rec_page_code and im.mct_name =ir.mct_name and im.first_cat_name=ir.first_cat_name
left join click_tmp c on im.pt=c.pt and im.rec_page_code = c.rec_page_code and im.mct_name =c.mct_name and im.first_cat_name=c.first_cat_name
left join click_recall_tmp cr on im.pt=cr.pt and im.rec_page_code = cr.rec_page_code and im.mct_name =cr.mct_name and im.first_cat_name=cr.first_cat_name
left join pay_tmp p on im.pt=p.pt and im.rec_page_code = p.rec_page_code and im.mct_name =p.mct_name and im.first_cat_name=p.first_cat_name
left join pay_recall_tmp pr on im.pt=pr.pt and im.rec_page_code = pr.rec_page_code and im.mct_name =pr.mct_name and im.first_cat_name=pr.first_cat_name;
"
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=tmp_vova_six_sale_d" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi



