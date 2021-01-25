#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="

with tmp1 as
(
select datasource,platform,search_word,search_uv,search_pv from
(
select nvl(datasource,'all')datasource,nvl(platform,'all')platform,nvl(search_word,'all')search_word ,count(distinct device_id) search_uv,count(1) search_pv from
(
select nvl(datasource,'NA') datasource,nvl(os_type,'NA') platform,lower(trim(element_id)) search_word,device_id from dwd.dwd_vova_log_common_click where pt='${cur_date}' and element_name='search_confirm'  and page_code in('search_begin','search_result') and (element_id is not null and element_id !='') and os_type in('android','ios')
)t1 group by datasource,platform,search_word with cube
)t2 where search_pv>10
),

tmp2 as
(
select datasource,platform,search_word,brand_status,is_brand,impr_uv,impr_pv,impr_goods from
(
select datasource,platform,search_word,brand_status,is_brand,impr_uv,impr_pv,impr_goods,row_number() over (partition by datasource,platform,search_word,brand_status,is_brand order by impr_uv desc) as rank from
(
select nvl(datasource,'all')datasource,nvl(platform,'all')platform,nvl(search_word,'all')search_word,nvl(brand_status,'all') brand_status,nvl(is_brand,'all') is_brand,count(distinct device_id)impr_uv,count(1) impr_pv,count(distinct virtual_goods_id) impr_goods from
(
select nvl(a.datasource,'NA') datasource,nvl(a.os_type,'NA') platform,lower(trim(a.element_type)) search_word,if(get_rp_name(a.recall_pool) like '%47%','Y','N') brand_status,if(b.brand_id >0,'Y','N') is_brand,a.device_id,a.virtual_goods_id
from dwd.dwd_vova_log_goods_impression a
left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
where pt='${cur_date}' and  list_type in('/search_result_recommend','/search_result_sold','/search_result_price_asc','/search_result_price_desc') and (element_type is not null and element_type != '') and os_type in('android','ios')
)t group by datasource,platform,search_word,brand_status,is_brand with cube
)t1
)t2 where rank=1
),

tmp22 as
(
select datasource,platform,search_word,brand_status,is_brand,click_uv,click_pv from
(
select datasource,platform,search_word,brand_status,is_brand,click_uv,click_pv,row_number() over (partition by datasource,platform,search_word,brand_status,is_brand order by click_uv desc) as rank from
(
select nvl(datasource,'all')datasource,nvl(platform,'all')platform,nvl(search_word,'all')search_word,nvl(brand_status,'all') brand_status,nvl(is_brand,'all') is_brand,count(distinct device_id)click_uv,count(1) click_pv from
(
select nvl(a.datasource,'NA') datasource,nvl(a.os_type,'NA') platform,lower(trim(a.element_type)) search_word,if(get_rp_name(a.recall_pool) like '%47%','Y','N') brand_status,if(b.brand_id >0,'Y','N') is_brand,a.device_id
from dwd.dwd_vova_log_goods_click a
left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
where pt='${cur_date}' and  list_type in('/search_result_recommend','/search_result_sold','/search_result_price_asc','/search_result_price_desc') and (element_type is not null and element_type != '') and os_type in('android','ios')
)t group by datasource,platform,search_word,brand_status,is_brand with cube
)t1
)t2 where rank=1
),

tmp3 as
(
select nvl(datasource,'all')datasource,nvl(platform,'all')platform,nvl(search_word,'all')search_word,nvl(brand_status,'all') brand_status,nvl(is_brand,'all') is_brand,count(distinct device_id) cart_uv from
(
select nvl(a.datasource,'NA')datasource,nvl(a.platform,'NA') platform,lower(trim(a.pre_element_type)) search_word,if(get_rp_name(a.pre_recall_pool) like '%47%','Y','N') brand_status,if(b.brand_id >0,'Y','N') is_brand,a.device_id
from dwd.dwd_vova_fact_cart_cause_v2 a
left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
where pt='${cur_date}' and pre_page_code='search_result' and (pre_element_type is not null and pre_element_type != '')
)t1 group by datasource,platform,search_word,brand_status,is_brand with cube
),

tmp4 as
(
select nvl(oc.datasource,'all')datasource,nvl(oc.platform,'all')platform,nvl(search_word,'all')search_word,nvl(brand_status,'all') brand_status,nvl(is_brand,'all') is_brand,count(distinct py.device_id) pay_uv,sum(py.shipping_fee + py.goods_number * py.shop_price) gmv
from (select * from  dwd.dwd_vova_fact_pay py where date(pay_time)='${cur_date}' and (from_domain like '%api.vova%' or from_domain like '%api.airyclub%') )py
inner join (select nvl(a.datasource,'NA')datasource,nvl(a.platform,'NA')platform,lower(trim(a.pre_element_type)) search_word,if(get_rp_name(a.pre_recall_pool) like '%47%','Y','N') brand_status,if(b.brand_id >0,'Y','N') is_brand,a.order_goods_id
from dwd.dwd_vova_fact_order_cause_v2 a
left join dim.dim_vova_goods b on a.goods_id = b.goods_id
where pt='${cur_date}' and pre_page_code='search_result' and (pre_element_type is not null and pre_element_type != ''))oc on py.order_goods_id=oc.order_goods_id
group by oc.datasource,oc.platform,search_word,brand_status,is_brand with cube
),

tmp5 as
(
select query,max(goods_cnt) goods_cnt from dwd.dwd_vova_rec_search_log where pt='${cur_date}' group by query
)

insert overwrite table dwb.dwb_vova_search_goods_report partition(pt='${cur_date}')
select '${cur_date}' event_date,tmp2.search_word,nvl(tmp1.search_pv,0) search_pv,nvl(tmp1.search_uv,0) search_uv,
nvl(tmp22.click_uv,0)click_uv,nvl(tmp2.impr_uv,0)impr_uv,nvl(tmp22.click_pv,0)click_pv,nvl(tmp2.impr_pv,0) impr_pv,
nvl(tmp3.cart_uv,0) cart_uv,
nvl(tmp4.pay_uv,0) pay_uv,
nvl(tmp4.gmv,0) gmv,
nvl(tmp2.impr_goods,0) impr_goods,
tmp2.datasource,
tmp2.platform,
nvl(tmp5.goods_cnt,0) goods_cnt,tmp2.brand_status,tmp2.is_brand
from tmp2 join tmp1 on tmp2.datasource=tmp1.datasource and tmp2.platform=tmp1.platform and tmp2.search_word=tmp1.search_word
left join tmp22 on tmp2.datasource=tmp22.datasource and tmp2.platform=tmp22.platform and tmp2.search_word=tmp22.search_word and tmp2.brand_status = tmp22.brand_status and tmp2.is_brand = tmp22.is_brand
left join tmp3 on tmp2.datasource=tmp3.datasource and tmp2.platform=tmp3.platform and tmp2.search_word=tmp3.search_word and tmp2.brand_status = tmp3.brand_status and tmp2.is_brand = tmp3.is_brand
left join tmp4 on tmp2.datasource=tmp4.datasource and tmp2.platform=tmp4.platform and tmp2.search_word=tmp4.search_word and tmp2.brand_status = tmp4.brand_status  and tmp2.is_brand = tmp4.is_brand
left join tmp5 on tmp2.search_word=tmp5.query
"

spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_search_goods_report" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ]; then
  echo "搜索商品统计${cur_date}错误"
  exit 1
fi



