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
DROP TABLE IF EXISTS tmp.vova_rec_page_code_report;
CREATE TABLE IF NOT EXISTS tmp.vova_rec_page_code_report STORED AS PARQUETFILE as
select
/*+ REPARTITION(20) */
t1.datasource,
t1.country,
t1.os_type,
t1.rec_page_code,
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
nvl((t4.payed_uv/t1.expre_uv-t5.payed_uv/t5.expre_uv)/(t5.payed_uv/t5.expre_uv),0) payed_uv_div_expre_uv_mom,
t1.is_brand,t1.brand_status
from
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
sum(expres) as expres,
sum(clks) as clks,
count(distinct device_id_clk) as clk_uv,
count(distinct device_id_expre) as expre_uv
from dwd.dwd_vova_rec_report_clk_expre where pt = '$cur_date'
group by
datasource,
country,
os_type,
rec_page_code,
is_brand,
brand_status
with cube
) t1
left join
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
count(distinct device_id)  as cart_uv
from
dwd.dwd_vova_rec_report_cart_cause where pt = '$cur_date'
group by
datasource,
country,
os_type,
rec_page_code,
is_brand,
brand_status
with cube
) t2 on t1.datasource = t2.datasource and t1.country =t2.country and t1.os_type = t2.os_type and t1.rec_page_code = t2.rec_page_code  and t1.is_brand = t2.is_brand and t1.brand_status = t2.brand_status
left join
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
count(order_goods_id)  as order_number
from
dwd.dwd_vova_rec_report_order_cause where pt = '$cur_date'
group by
datasource,
country,
os_type,
rec_page_code,
is_brand,brand_status
with cube
) t3 on t1.datasource = t3.datasource and t1.country =t3.country and t1.os_type = t3.os_type and t1.rec_page_code = t3.rec_page_code  and t1.is_brand = t3.is_brand and t1.brand_status = t3.brand_status
left join
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv
from
dwd.dwd_vova_rec_report_pay_cause where pt = '$cur_date'
group by
datasource,
country,
os_type,
rec_page_code,
is_brand,brand_status
with cube
) t4 on t1.datasource = t4.datasource and t1.country =t4.country and t1.os_type = t4.os_type and t1.rec_page_code = t4.rec_page_code and t1.is_brand = t4.is_brand and t1.brand_status = t4.brand_status
left join
(
select
datasource,
country,
os_type,
rec_page_code,
is_brand,
brand_status,
expre_uv,
gmv,
cart_uv,
payed_uv
from dwb.dwb_vova_rec_report where pt='$pre_date' and page_code='NA' and list_type='NA' and (activate_time = 'all' or activate_time = '')
) t5 on t1.datasource = t5.datasource and t1.country =t5.country and t1.os_type = t5.os_type and t1.rec_page_code = t5.rec_page_code  and t1.is_brand = t5.is_brand and t1.brand_status = t5.brand_status
;



drop table if exists tmp.vova_rec_report_01;
create table tmp.vova_rec_report_01  STORED AS PARQUETFILE as
select
/*+ REPARTITION(1) */
t1.datasource,
t1.country,
t1.os_type,
t1.page_code,
t1.list_type,
nvl(t1.expres,0) expres,
nvl(t1.clks,0) clks,
nvl(t1.clk_uv,0) clk_uv,
nvl(t1.expre_uv,0) expre_uv,
nvl(t2.cart_uv,0) cart_uv,
nvl(t3.order_number,0) order_number,
nvl(t4.payed_number,0) payed_number,
nvl(t4.payed_uv,0) payed_uv,
nvl(t4.gmv,0) gmv,
nvl(t4.payed_gds_num,0) payed_gds_num,
t1.is_brand,
t1.brand_status
from
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
sum(expres) as expres,
sum(clks) as clks,
count(distinct device_id_clk) as clk_uv,
count(distinct device_id_expre) as expre_uv
from dwd.dwd_vova_rec_report_clk_expre  where pt = '$cur_date'
group by
datasource,
country,
os_type,
page_code,
list_type,
is_brand,brand_status
with cube
) t1
left join
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
count(distinct device_id)  as cart_uv
from
dwd.dwd_vova_rec_report_cart_cause  where pt = '$cur_date'
group by
datasource,
country,
os_type,
page_code,
list_type,
is_brand,brand_status
with cube
) t2 on t1.datasource = t2.datasource and t1.country =t2.country and t1.os_type = t2.os_type and t1.page_code = t2.page_code and t1.list_type =t2.list_type
AND t1.is_brand = t2.is_brand and t1.brand_status = t2.brand_status
left join
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
count(order_goods_id)  as order_number
from
dwd.dwd_vova_rec_report_order_cause where pt = '$cur_date'
group by
datasource,
country,
os_type,
page_code,
list_type,
is_brand,brand_status
with cube
) t3 on t1.datasource = t3.datasource and t1.country =t3.country and t1.os_type = t3.os_type and t1.page_code = t3.page_code and t1.list_type = t3.list_type
AND t1.is_brand = t3.is_brand  and t1.brand_status = t3.brand_status
left join
(
select
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(list_type,'all') list_type,
nvl(is_brand,'all') is_brand,
nvl(brand_status,'all') brand_status,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv,
sum(goods_number) as payed_gds_num
from
dwd.dwd_vova_rec_report_pay_cause where pt = '$cur_date'
group by
datasource,
country,
os_type,
page_code,
list_type,
is_brand,brand_status
with cube
) t4 on t1.datasource = t4.datasource and t1.country =t4.country and t1.os_type = t4.os_type and t1.page_code = t4.page_code and t1.list_type = t4.list_type
AND t1.is_brand = t4.is_brand  and t1.brand_status = t4.brand_status
;

insert overwrite table dwb.dwb_vova_rec_report  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
datasource,
country,
os_type,
rec_page_code,
'NA' page_code,
'NA' list_type,
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
nvl(cart_uv_div_expre_uv_mom,0) cart_uv_div_expre_uv_mom,
'all',
is_brand,
brand_status
from tmp.vova_rec_page_code_report
union all
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
datasource,
country,
os_type,
'NA' rec_page_code,
page_code,
list_type,
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
0 cart_uv_div_expre_uv_mom,
'all',
is_brand,
brand_status
from tmp.vova_rec_report_01;

"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_rec_report" \
--conf "spark.default.parallelism = 430" \
--conf "spark.sql.shuffle.partitions=430" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi








