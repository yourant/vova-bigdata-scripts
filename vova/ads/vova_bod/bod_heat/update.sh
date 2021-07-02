#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
insert overwrite table ads.ads_vova_bod_heat_rank partition (pt = '${cur_date}')
select bod_id,row_number() over(order by expre_rate desc) as rank from (
select a.bod_id,
sum(case when b.gmv_1w=0.00 or b.expre_cnt_1w=0 then 0 else round(b.gmv_1w*10000/b.expre_cnt_1w,6) end) as expre_rate
from (
  select bod_id,goods_id from
  ads.ads_vova_knowledge_graph_bod_goods_rank_data
  where pt = '${cur_date}'
  union all
  select bod_id,goods_id from
  ads.ads_vova_scene_bod_goods_rank_data
  where pt = '${cur_date}'
) a
left join ads.ads_vova_goods_portrait b on a.goods_id=b.goods_id
where b.pt='${cur_date}'
group by a.bod_id
) t;
"
spark-sql --conf "spark.app.name=ads_vova_bod_heat_rank" --conf "spark.dynamicAllocation.maxExecutors=50" -e "$sql"
#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi