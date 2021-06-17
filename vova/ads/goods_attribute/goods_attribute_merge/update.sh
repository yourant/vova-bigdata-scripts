#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

reg='\\&|\\"|\\/|\\^|#|\\\n|\\\t|\\\r|\\|,|,|，|`|\\;|!|\\[|\\]|\\+|\\*|\\?|:|。|《|》|\\<|\\>|_|\\{|\\}\\~|\\@|\\¥|=|、|%|\\$'

regt="\'s|）|\\)|_\\*"


sql="
with tmp_vova_goods_attribute as (
select *
from (
         select distinct a.goods_id,
                         case
                             when second_cat_id in (5743, 5954, 5741) then 7
                             when second_cat_id in
                                  (5713, 5973, 5975, 5731, 5732, 5733, 5735, 5736, 5737, 5974, 5890, 5891, 5831)
                                 then 8
                             when second_cat_id in
                                  (5712, 5722, 5723, 5726, 5775, 5778, 5819, 5827, 5952, 5979, 5982, 5983, 5984, 5716,
                                   5717,
                                   5718, 5721, 6025) then 3
                             when second_cat_id in
                                  (5784, 5795, 5768, 5785, 5786, 5787, 5789, 5790, 5792, 5793, 5794, 5987)
                                 then 10
                             when second_cat_id in (5977, 5788, 5927, 6006, 6007, 6008) then 4
                             when second_cat_id in
                                  (164, 165, 166, 171, 173, 194, 195, 3001, 3004, 5928, 5929, 5930, 5932, 5933, 5934,
                                   5935, 5938,
                                   5960, 5961, 5962, 5963, 6009) then 9
                             when second_cat_id in
                                  (5825, 5902, 5903, 5904, 5905, 5906, 5907, 5909, 5911, 5939, 5941, 5942, 5943, 5944,
                                   5964,
                                   5965, 5966, 5967) then 11
                             when second_cat_id in (5773) then 2
                             when second_cat_id in (5711, 5976, 5978, 5990, 5991, 5992, 5993, 5994) then 5
                             when second_cat_id in (5777, 5882, 5883, 5968, 5969, 5970, 5971, 5972) then 6
                             when second_cat_id in (5821, 5818) then 12
                             when second_cat_id in
                                  (5796, 5797, 5798, 5799, 5800, 5801, 5802, 5803, 5804, 5805, 5807, 5808, 5986) then 13
                             when second_cat_id in (5810, 5811,5812,5813,5814,5815,5816,5988 ) then 14
                             when second_cat_id in (5809) then 15
                             else 0
                             end            as class_id,
                         regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(b.name), ' |\\\\|/|\\(', '_'), '__|（|-', '_'), '${regt}',
                                        ''),'__','_') as name,
                         concat_ws(' ', sentences(
                                 lower(
                                         trim(
                                                 regexp_replace(
                                                         regexp_replace(c.value,
                                                                        '${reg}',
                                                                        ' ')
                                                     , '[\\\s]+', ' ')
                                             )
                                     ))[0]) as value
         from ods_vova_vteos.ods_vova_goods_attributes a
                  inner join dim.dim_vova_goods dg
                             on dg.goods_id = a.goods_id
                      left join ods_vova_vteos.ods_vova_attributes_name b
                                on a.name_id = b.id
                      left join ods_vova_vteos.ods_vova_attributes_value c
                                on c.id = a.value_id
    where a.is_delete = 0
    and b.is_delete = 0
    and c.is_delete = 0
     ) t1
where class_id != 0
)

insert overwrite table ads.ads_vova_goods_attribute_merge PARTITION (pt='${cur_date}')
select
    a.*,
    dg.first_cat_id,
    dg.second_cat_id,
    dg.third_cat_id,
    dg.fourth_cat_id,
    dg.cat_id,
    dg.brand_id,
    dg.goods_sn
from (
         select
             distinct
             nvl(a.goods_id,b.goods_id) as goods_id,
             nvl(a.class_id,b.cat_attr_id) as cat_attr_id,
             nvl(a.name,b.attr_key) as attr_key,
             nvl(a.value,b.attr_value) as attr_value
         from
             tmp_vova_goods_attribute a
                 full outer join
             ads.ads_vova_goods_attribute_label_data b
             on a.goods_id = b.goods_id
                 and a.name = b.attr_key
                 and a.value = b.attr_value
     ) a
         left join dim.dim_vova_goods dg
                   on a.goods_id = dg.goods_id
;
"

spark-sql \
--conf "spark.app.name=ads_vova_goods_attribute_merge" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi