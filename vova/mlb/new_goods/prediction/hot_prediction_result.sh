#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$cur_date"

sql="
msck repair table mlb.mlb_vova_hot_prediction_result_data;
INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_predicte_result PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
fin.goods_id,
dg.cat_id,
fin.prob_score as predicte_score,
row_number() over(order by fin.prob_score desc) rank
FROM
(
select
d1.goods_id,
d1.prob_score,
d1.pt,
row_number() over(partition by d1.pt order by d1.prob_score desc ) rank2
from
(
select
d1.goods_id,
d1.prob_score,
d1.pt,
row_number() over(partition by d1.goods_id order by d1.pt desc, d1.prob_score desc ) rank
from
mlb.mlb_vova_hot_prediction_result_data d1
left join (
select
distinct good_id
from
mlb.mlb_vova_hot_goods_prediction_base
WHERE is_hot = 1
) pred_hot on pred_hot.good_id = d1.goods_id
where d1.pt >= date_sub('${cur_date}', 14)
and d1.pt <= '${cur_date}'
and d1.goods_id != 'goods_id'
and pred_hot.good_id is null
) d1
where rank = 1
) fin
inner join dim.dim_vova_goods dg on dg.goods_id = fin.goods_id
where fin.rank2 <= 10
order by fin.prob_score desc
;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=mlb_vova_new_goods_predicte_result" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

