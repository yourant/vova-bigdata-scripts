#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table dim.dim_vova_virtual_six_mct_goods
select /*+ REPARTITION(1) */
t.goods_id,
g.virtual_goods_id,
-1000 mct_id,
'竞网头部跟卖' mct_name,
g.first_cat_id,
g.second_cat_id,
g.cat_id,
g.brand_id,
g.group_id
from
(
select
goods_id
from  ods_vova_vbd.ods_vova_test_goods_behave
where test_result !=2 and select_cat_channel in ('中台选品','AE达标款')
group by goods_id
) t left join dim.dim_vova_goods g on t.goods_id = g.goods_id;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_vova_virtual_six_mct_goods"  --conf "spark.dynamicAllocation.maxExecutors=50" -e "$sql"

#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi