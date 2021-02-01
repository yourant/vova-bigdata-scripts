#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "7 day ago ${cur_date}" +%Y-%m-%d`
pre_date=`date -d "1 day ago ${cur_date}" +%Y-%m-%d`
###逻辑sql
sql="
drop table if exists tmp.tmp_dwb_vova_web_rec_report_clk_expre;
create table tmp.tmp_dwb_vova_web_rec_report_clk_expre as
select
/*+ REPARTITION(5) */
nvl(gc.datasource,'NA') datasource,
nvl(gc.geo_country,'NA') country,
nvl(gc.platform,'NA') platform,
nvl(gc.page_code,'NA') page_code,
nvl(gc.list_type,'NA') list_type,
nvl(voc.original_channel, 'unknown') AS original_channel,
case when page_code = 'homepage' and list_type='/popular' then 'rec_best_selling'
     when page_code in ('product_list') and  list_type in ('/product_list_popular') then 'rec_most_popular'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result') then 'rec_search_result'
     when page_code ='activity_list' then 'rec_activity'
     else 'others' end rec_page_code,
gc.domain_userid domain_userid_clk,
null domain_userid_expre,
1 clks,
0 expres
from dwd.dwd_vova_log_goods_click gc
LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = gc.domain_userid AND voc.datasource = gc.datasource
where gc.pt='$cur_date'
and gc.platform in('pc','web')
and gc.dp = 'vova'
and gc.datasource = 'vova'
union all
select
/*+ REPARTITION(10) */
nvl(gi.datasource,'NA') datasource,
nvl(gi.geo_country,'NA') country,
nvl(gi.platform,'NA') platform,
nvl(gi.page_code,'NA') page_code,
nvl(gi.list_type,'NA') list_type,
nvl(voc.original_channel, 'unknown') AS original_channel,
case when page_code = 'homepage' and list_type='/popular' then 'rec_best_selling'
     when page_code in ('product_list') and  list_type in ('/product_list_popular') then 'rec_most_popular'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result') then 'rec_search_result'
     when page_code ='activity_list' then 'rec_activity'
     else 'others' end rec_page_code,
null domain_userid_clk,
gi.domain_userid domain_userid_expre,
0 clks,
1 expres
from dwd.dwd_vova_log_goods_impression gi
LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = gi.domain_userid AND voc.datasource = gi.datasource
where gi.pt='$cur_date'
and gi.platform in('pc','web')
and gi.dp = 'vova'
and gi.datasource = 'vova'
;

drop table if exists tmp.tmp_dwb_vova_web_rec_report_cart_cause;
create table tmp.tmp_dwb_vova_web_rec_report_cart_cause as
select
/*+ REPARTITION(1) */
nvl(c.datasource,'NA') datasource,
nvl(c.platform,'NA') platform,
nvl(c.country,'NA') country,
nvl(pre_page_code,'NA') page_code,
nvl(pre_list_type,'NA') list_type,
nvl(voc.original_channel, 'unknown') AS original_channel,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('product_list') and  pre_list_type in ('/product_list_popular') then 'rec_most_popular'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result') then 'rec_search_result'
     when pre_page_code ='activity_list' then 'rec_activity'
     else 'others' end rec_page_code,
c.domain_userid
from dwd.dwd_vova_web_fact_cart_cause c
LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = c.domain_userid AND voc.datasource = c.datasource
where pt='$cur_date' and pre_page_code is not null;

drop table if exists tmp.tmp_dwb_vova_web_rec_report_order_cause;
create table tmp.tmp_dwb_vova_web_rec_report_order_cause as
select
/*+ REPARTITION(1) */
nvl(zog.datasource,'NA') datasource,
nvl(zog.region_code,'NA') country,
nvl(if(zog.from_domain like '%api%', 'web', 'pc'),'NA') platform,
nvl(pre_page_code,'NA') page_code,
nvl(pre_list_type,'NA') list_type,
nvl(voc.original_channel, 'unknown') AS original_channel,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('product_list') and  pre_list_type in ('/product_list_popular') then 'rec_most_popular'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result') then 'rec_search_result'
     when pre_page_code ='activity_list' then 'rec_activity'
     else 'others' end rec_page_code,
zog.buyer_id,
zog.order_goods_id
from
dim.dim_vova_order_goods zog
left join dwd.dwd_vova_web_fact_order_cause oc on zog.order_goods_id = oc.order_goods_id
LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = zog.device_id AND voc.datasource = zog.datasource
where date(zog.order_time) ='$cur_date' and oc.pt='$cur_date'
        AND zog.datasource = 'vova'
        AND zog.from_domain not like '%api%'
and oc.pre_page_code is not null;


drop table if exists tmp.tmp_dwb_vova_web_rec_report_pay_cause;
create table tmp.tmp_dwb_vova_web_rec_report_pay_cause as
select
/*+ REPARTITION(1) */
nvl(zog.datasource,'NA') datasource,
nvl(zog.region_code,'NA') country,
nvl(if(zog.from_domain like '%api%', 'web', 'pc'),'NA') platform,
nvl(pre_page_code,'NA') page_code,
nvl(pre_list_type,'NA') list_type,
nvl(voc.original_channel, 'unknown') AS original_channel,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('product_list') and  pre_list_type in ('/product_list_popular') then 'rec_most_popular'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result') then 'rec_search_result'
     when pre_page_code ='activity_list' then 'rec_activity'
     else 'others' end rec_page_code,
zog.buyer_id,
zog.order_goods_id,
zog.goods_number * zog.shop_price as gmv
from dim.dim_vova_order_goods zog
left join dwd.dwd_vova_web_fact_order_cause oc on zog.order_goods_id = oc.order_goods_id
LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = zog.device_id AND voc.datasource = zog.datasource
where date(zog.pay_time) ='$cur_date' and (oc.pt>='$pre_week' and oc.pt<='$cur_date')
        AND zog.datasource = 'vova'
        AND zog.from_domain not like '%api%'
and oc.pre_page_code is not null;

drop table if exists tmp.tmp_dwb_vova_web_rec_page_code_report;
create table tmp.tmp_dwb_vova_web_rec_page_code_report as
select
/*+ REPARTITION(1) */
t1.datasource,
t1.country,
t1.platform,
t1.rec_page_code,
t1.original_channel,
nvl(t1.expres,0) expres,
nvl(t1.clks,0) clks,
nvl(t1.clk_uv,0) clk_uv,
nvl(t1.expre_uv,0) expre_uv,
nvl(t2.cart_uv,0) cart_uv,
nvl(t3.order_number,0) order_number,
nvl(t4.payed_number,0) payed_number,
nvl(t4.payed_uv,0) payed_uv,
nvl(t4.gmv,0) gmv,
nvl(t2.cart_uv/t1.expre_uv,0) cart_uv_div_expre_uv,
nvl(t4.payed_uv/t1.expre_uv,0) payed_uv_div_expre_uv,
nvl((t4.gmv-t5.gmv)/t5.gmv,0) gmv_mom,
nvl((t2.cart_uv/t1.expre_uv-t5.cart_uv/t5.expre_uv)/(t5.cart_uv/t5.expre_uv),0)  cart_uv_div_expre_uv_mom,
nvl((t4.payed_uv/t1.expre_uv-t5.payed_uv/t5.expre_uv)/(t5.payed_uv/t5.expre_uv),0) payed_uv_div_expre_uv_mom
from
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(rec_page_code,'all') rec_page_code,
nvl(original_channel,'all') original_channel,
sum(expres) as expres,
sum(clks) as clks,
count(distinct domain_userid_clk) as clk_uv,
count(distinct domain_userid_expre) as expre_uv
from tmp.tmp_dwb_vova_web_rec_report_clk_expre t1
group by
t1.datasource,
country,
platform,
original_channel,
rec_page_code
with cube
) t1
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(rec_page_code,'all') rec_page_code,
nvl(original_channel,'all') original_channel,
count(distinct domain_userid)  as cart_uv
from
tmp.tmp_dwb_vova_web_rec_report_cart_cause t1
group by
t1.datasource,
country,
platform,
original_channel,
rec_page_code
with cube
) t2 on t1.datasource = t2.datasource and t1.country =t2.country and t1.platform = t2.platform and t1.rec_page_code = t2.rec_page_code and t1.original_channel = t2.original_channel
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(rec_page_code,'all') rec_page_code,
nvl(original_channel,'all') original_channel,
count(order_goods_id)  as order_number
from
tmp.tmp_dwb_vova_web_rec_report_order_cause t1
group by
t1.datasource,
country,
platform,
original_channel,
rec_page_code
with cube
) t3 on t1.datasource = t3.datasource and t1.country =t3.country and t1.platform = t3.platform and t1.rec_page_code = t3.rec_page_code and t1.original_channel = t3.original_channel
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(rec_page_code,'all') rec_page_code,
nvl(original_channel,'all') original_channel,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv
from
tmp.tmp_dwb_vova_web_rec_report_pay_cause t1
group by
t1.datasource,
country,
platform,
original_channel,
rec_page_code
with cube
) t4 on t1.datasource = t4.datasource and t1.country =t4.country and t1.platform = t4.platform and t1.rec_page_code = t4.rec_page_code and t1.original_channel = t4.original_channel
left join
(
select
datasource,
country,
platform,
rec_page_code,
original_channel,
expre_uv,
gmv,
cart_uv,
payed_uv
from dwb.dwb_vova_web_rec_report where pt='$pre_date' and page_code='NA' and list_type='NA' AND datasource IN ('vova')
) t5 on t1.datasource = t5.datasource and t1.country =t5.country and t1.platform = t5.platform and t1.rec_page_code = t5.rec_page_code and t1.original_channel = t5.original_channel

;

drop table if exists tmp.tmp_dwb_vova_web_rec_report;
create table tmp.tmp_dwb_vova_web_rec_report as
select
/*+ REPARTITION(1) */
t1.datasource,
t1.country,
t1.platform,
t1.page_code,
t1.list_type,
t1.original_channel,
nvl(t1.expres,0) expres,
nvl(t1.clks,0) clks,
nvl(t1.clk_uv,0) clk_uv,
nvl(t1.expre_uv,0) expre_uv,
nvl(t2.cart_uv,0) cart_uv,
nvl(t3.order_number,0) order_number,
nvl(t4.payed_number,0) payed_number,
nvl(t4.payed_uv,0) payed_uv,
nvl(t4.gmv,0) gmv
from
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(original_channel,'all') original_channel,
sum(expres) as expres,
sum(clks) as clks,
count(distinct domain_userid_clk) as clk_uv,
count(distinct domain_userid_expre) as expre_uv
from tmp.tmp_dwb_vova_web_rec_report_clk_expre t1
group by
t1.datasource,
country,
platform,
page_code,
original_channel,
list_type
with cube
) t1
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(original_channel,'all') original_channel,
count(distinct domain_userid)  as cart_uv
from
tmp.tmp_dwb_vova_web_rec_report_cart_cause t1
group by
t1.datasource,
country,
platform,
page_code,
original_channel,
list_type
with cube
) t2 on t1.datasource = t2.datasource and t1.country =t2.country and t1.platform = t2.platform and t1.page_code = t2.page_code and t1.list_type =t2.list_type and t1.original_channel = t2.original_channel
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(original_channel,'all') original_channel,
count(order_goods_id)  as order_number
from
tmp.tmp_dwb_vova_web_rec_report_order_cause t1
group by
t1.datasource,
country,
platform,
page_code,
original_channel,
list_type
with cube
) t3 on t1.datasource = t3.datasource and t1.country =t3.country and t1.platform = t3.platform and t1.page_code = t3.page_code and t1.list_type = t3.list_type and t1.original_channel = t3.original_channel
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(country,'all') country,
nvl(platform,'all') platform,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(original_channel,'all') original_channel,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv
from
tmp.tmp_dwb_vova_web_rec_report_pay_cause t1
group by
t1.datasource,
country,
platform,
page_code,
original_channel,
list_type
with cube
) t4 on t1.datasource = t4.datasource and t1.country =t4.country and t1.platform = t4.platform and t1.page_code = t4.page_code and t1.list_type = t4.list_type and t1.original_channel = t4.original_channel

;

insert overwrite table dwb.dwb_vova_web_rec_report  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
datasource,
country,
platform,
rec_page_code,
'NA' page_code,
'NA' list_type,
original_channel,
nvl(expres,0) expres,
nvl(clks,0) clks,
nvl(clk_uv,0) clk_uv,
nvl(expre_uv,0) expre_uv,
nvl(cart_uv,0) cart_uv,
nvl(order_number,0) order_number,
nvl(payed_number,0) payed_number,
nvl(payed_uv,0) payed_uv,
nvl(gmv,0) gmv,
nvl(cart_uv_div_expre_uv,0) cart_uv_div_expre_uv,
nvl(payed_uv_div_expre_uv,0) payed_uv_div_expre_uv,
nvl(gmv_mom,0) gmv_mom,
nvl(payed_uv_div_expre_uv_mom,0) payed_uv_div_expre_uv_mom,
nvl(cart_uv_div_expre_uv_mom,0) cart_uv_div_expre_uv_mom
from tmp.tmp_dwb_vova_web_rec_page_code_report
union all
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
datasource,
country,
platform,
'NA' rec_page_code,
page_code,
list_type,
original_channel,
nvl(expres,0) expres,
nvl(clks,0) clks,
nvl(clk_uv,0) clk_uv,
nvl(expre_uv,0) expre_uv,
nvl(cart_uv,0) cart_uv,
nvl(order_number,0) order_number,
nvl(payed_number,0) payed_number,
nvl(payed_uv,0) payed_uv,
nvl(gmv,0) gmv,
0 cart_uv_div_expre_uv,
0 payed_uv_div_expre_uv,
0 gmv_mom,
0 payed_uv_div_expre_uv_mom,
0 cart_uv_div_expre_uv_mom
from tmp.tmp_dwb_vova_web_rec_report;

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=dwb_vova_web_rec_report" -e "$sql"
#hive -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

