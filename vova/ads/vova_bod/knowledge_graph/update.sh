#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
with
kg_data as (
    select goods_id, attr_key key, t1.attr_value v, second_cat_id from (
        (select * from ads.ads_vova_goods_attribute_merge where pt = '${cur_date}') t1
        inner join
        (select * from ads.ads_vova_usable_value) t2
        on t1.attr_value = t2.attr_value
    )
),

tmp_group_score as (
select group_id,goods_id,overall_score from (
select a.goods_id,a.group_id,b.overall_score,row_number() over(partition by a.group_id order by b.overall_score desc) as rank from dim.dim_vova_goods a
left join mlb.mlb_vova_rec_b_goods_score_d b on a.goods_id=b.goods_id
where b.pt='${cur_date}' and a.group_id!=-1) t where rank=1
),

tmp_goods_score as (
	select goods_id,target_goods_id,overall_score from (
	select a.goods_id,b.goods_id as target_goods_id,b.overall_score from dim.dim_vova_goods a
	inner join ads.ads_vova_goods_portrait c on a.goods_id=c.gs_id
	left join tmp_group_score b on a.group_id=b.group_id
	where a.group_id!=-1 and c.pt='${cur_date}' and c.is_recommend = 1
	union all
	select a.goods_id,a.goods_id as target_goods_id,b.overall_score from dim.dim_vova_goods a
	inner join ads.ads_vova_goods_portrait c on a.goods_id=c.gs_id
	left join mlb.mlb_vova_rec_b_goods_score_d b on a.goods_id=b.goods_id
	where b.pt='${cur_date}' and a.group_id=-1 and c.pt='${cur_date}' and c.is_recommend = 1
	)
),
tmp_bod_goods as (
  select
  distinct bod_id,goods_id,rank from (
  select t2.bod_id,t3.target_goods_id as goods_id,dense_rank() over(partition by t2.bod_id order by t3.overall_score desc) as rank from
      (
          select tt1.goods_id goods_id
                  ,concat_ws('|',tt1.v,tt2.v,tt1.second_cat_id) tags
          from kg_data as tt1, kg_data as tt2
          where tt1.goods_id = tt2.goods_id and tt1.key != tt2.key and tt1.v < tt2.v
      ) t1 inner join ads.ads_vova_bod t2 on t1.tags = t2.bod_name
      inner join tmp_goods_score t3 on t1.goods_id=t3.goods_id
  ) T where rank<=500
)

insert overwrite table ads.ads_vova_knowledge_graph_bod_goods_rank_data partition (pt = '${cur_date}')
select /*+ REPARTITION(1) */
a.bod_id,b.goods_id,b.rank from (
select bod_id from tmp_bod_goods
group by bod_id having count(goods_id)>=50
) a left join tmp_bod_goods b on a.bod_id=b.bod_id;
"
spark-sql --conf "spark.app.name=ads_vova_knowledge_graph_bod_goods_rank_data" --conf "spark.dynamicAllocation.maxExecutors=200" -e "$sql"
#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi