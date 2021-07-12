#!/bin/bash
#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

###逻辑sql
reg='\\|'
sql="
insert overwrite table ads.ads_vova_scene_bod_goods_rank_data partition(pt='${cur_date}')
select
/*+ repartition(1) */
bod_id,goods_id,rank from (
  select
  b.bod_id,a.goods_id,row_number() over(partition by b.bod_id order by c.overall_score desc) as rank
  from (
  select
      distinct bod_name,goods_id FROM ads.ads_vova_scene_bod_original_data
      lateral view explode(split(goods_list,'${reg}')) t AS goods_id
  ) a inner join ads.ads_vova_bod b on a.bod_name=b.bod_name
inner join mlb.mlb_vova_rec_b_goods_score_d c on a.goods_id=c.goods_id
  where c.pt='${cur_date}' and b.bod_type=6
) t where rank<=500;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=ads_vova_scene_bod_goods_rank_data" --conf "spark.dynamicAllocation.maxExecutors=50" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

