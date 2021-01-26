#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql  --conf "spark.app.name=dwb_vova_goods_attribute" --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=100"  -e "

--获取国家售卖商品数
DROP TABLE IF EXISTS tmp.tmp_vova_goods_attribute_tmp;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_goods_attribute_tmp as
select a.region_id,a.region_code,cnt.cnt,cnt.brand_cnt,cnt.no_brand_cnt
from
 (
 select region_id,
       region_code
from ods_vova_vts.ods_vova_region
where parent_id = 0
  and region_display = 1
  and region_type = 0
group by region_id,region_code
) a
left join
(
select
    count(distinct if(brand_id > 0,goods_id,null)) brand_cnt,
    count(distinct if(brand_id <= 0,goods_id,null)) no_brand_cnt,
    count(distinct goods_id) cnt
from dim.dim_vova_goods
where datasource = 'vova'
and is_on_sale = 1
) cnt
on 1=1
;

--获取国家禁售商品数
DROP TABLE IF EXISTS tmp.tmp_vova_goods_attribute_tmp_2;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_goods_attribute_tmp_2 as
select a.region_id,count(distinct a.goods_id) cnt,count(distinct if(dg.brand_id > 0,a.goods_id,null)) brand_cnt,count(distinct if(dg.brand_id <= 0,a.goods_id,null)) no_brand_cnt
from dwd.dwd_vova_fact_shield_goods a
join dim.dim_vova_goods dg
on a.goods_id = dg.goods_id
where dg.datasource = 'vova'
and dg.is_on_sale = 1
group by a.region_id
;

--获取全部禁售数
DROP TABLE IF EXISTS tmp.tmp_vova_goods_attribute_tmp_3;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_goods_attribute_tmp_3 as
select count(distinct a.goods_id) cnt,count(distinct if(dg.brand_id > 0,a.goods_id,null)) brand_cnt,count(distinct if(dg.brand_id <= 0,a.goods_id,null)) no_brand_cnt
from dwd.dwd_vova_fact_shield_goods a
join dim.dim_vova_goods dg
on a.goods_id = dg.goods_id
where dg.datasource = 'vova'
and dg.is_on_sale = 1
;

insert overwrite table dwb.dwb_vova_goods_attribute  PARTITION (pt = '${cur_date}')
select '${cur_date}' cur_date,
nvl(a.region_code,'NA') region_code,
max(a.cnt) - max(nvl(fsg.cnt,0)) on_sale_goods,
max(nvl(fsg.cnt,0)) shield_cnt,
max(nvl(fp.cnt,0)) pay_goods,
max(nvl(fp2.cnt,0)) onsale_pay_goods,
       max(a.brand_cnt) - max(nvl(fsg.brand_cnt,0)) on_sale_brand_goods,
       max(a.no_brand_cnt) - max(nvl(fsg.no_brand_cnt,0)) on_sale_no_brand_goods
from tmp.tmp_vova_goods_attribute_tmp a
left join tmp.tmp_vova_goods_attribute_tmp_2 fsg
on a.region_id = fsg.region_id
left join (select region_id,count(distinct goods_id) cnt from dwd.dwd_vova_fact_pay where to_date(pay_time) = '${cur_date}' group by region_id)  fp
on a.region_id = fp.region_id
left join (select a.region_id,count(distinct a.goods_id) cnt from  dwd.dwd_vova_fact_pay a join dim.dim_vova_goods b on a.goods_id = b.goods_id where b.datasource = 'vova' and b.is_on_sale = 1 and to_date(a.pay_time) = '${cur_date}' group by a.region_id) fp2
on a.region_id = fp2.region_id
group by nvl(a.region_code,'NA')
union all
select '${cur_date}' cur_date,
'all' region_code,
a.cnt  on_sale_goods,
fsg.cnt shield_cnt,
fp.cnt pay_goods,
fp2.cnt onsale_pay_goods,
       a.brand_cnt,
       a.no_brand_cnt
from (select count(distinct goods_id) cnt,count(distinct if(brand_id > 0,goods_id,null)) brand_cnt,count(distinct if(brand_id <= 0,goods_id,null)) no_brand_cnt from dim.dim_vova_goods where datasource = 'vova' and is_on_sale = 1) a
left join tmp.tmp_vova_goods_attribute_tmp_3 fsg
on 1 = 1
left join (select count(distinct goods_id) cnt from dwd.dwd_vova_fact_pay where to_date(pay_time) = '${cur_date}') fp
on 1 = 1
left join (select count(distinct a.goods_id) cnt from  dwd.dwd_vova_fact_pay a join dim.dim_vova_goods b on a.goods_id = b.goods_id where b.datasource = 'vova' and b.is_on_sale = 1 and to_date(a.pay_time) = '${cur_date}') fp2
on 1 = 1

"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi



