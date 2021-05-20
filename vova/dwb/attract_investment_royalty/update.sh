#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
cur_month="${cur_date: 0: 7}-01"

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
cur_month=$(date -d "-1 day" +"%Y-%m-01")
fi
# 当天日期
echo "cur_date: ${cur_date}"
#当月第一天
echo "cur_month: ${cur_month}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_royalty_norm_req9531_chenkai_${cur_date}"

###逻辑sql
sql="
create table if not exists tmp.tmp_vova_first_cat_group_gmv_${table_suffix} as
select /*+ REPARTITION(1) */
  first_cat_id,
  group_id,
  gmv,
  next_group,
  next_gmv,
  if(next_gmv > 0, round((gmv-next_gmv)/3, 4), 0) avg_diff, -- 商品组 gmv 与之后三个商品组 gmv 的差值的均值
  round(stddev(gmv) over(partition by first_cat_id), 4) gmv_stddev -- 一级品类的标准差
from
(
  select
    first_cat_id,
    group_id,
    gmv,
    lead(group_id, 3, 0) over(partition by first_cat_id order by gmv desc) next_group, -- 之后第三个分组
    lead(gmv, 3, 0) over(partition by first_cat_id order by gmv desc) next_gmv -- 之后第三个分组的gmv
  from
  (
    select
      fp.first_cat_id,
      rgps.group_id,
      sum(fp.shop_price * fp.goods_number + fp.shipping_fee) gmv
    from
      dwd.dwd_vova_fact_pay fp
    left join
      ods_vova_vbts.ods_vova_rec_gid_pic_similar rgps
    on fp.goods_id = rgps.goods_id
    where from_unixtime(to_unix_timestamp(fp.pay_time), 'yyyy-MM') = substr('${cur_month}',0,7)
    group by fp.first_cat_id, rgps.group_id
  ) t1
) t1
;

insert overwrite table dwb.dwb_vova_royalty_norm partition(pt='${cur_month}')
select /*+ REPARTITION(1) */
  t1.first_cat_id,
  regexp_replace(t2.first_cat_name, '\'', ' ') first_cat_name,
  t1.month_sale_threshold,
  t1.gmv_stddev,
  t1.group_id,
  t1.gmv,
  t1.next_group,
  t1.next_gmv,
  t1.avg_diff,
  t1.diff_stddev
from
(
  select
    first_cat_id   first_cat_id,
    gmv            month_sale_threshold,
    gmv_stddev     gmv_stddev,
    group_id       group_id,
    gmv            gmv,
    next_group     next_group,
    next_gmv       next_gmv,
    avg_diff       avg_diff,
    avg_diff - gmv_stddev diff_stddev,
    row_number() over(partition by first_cat_id order by gmv desc) rank -- 三个差值的均值 减 标准差后第一个小于0的值
  from
    tmp.tmp_vova_first_cat_group_gmv_${table_suffix}
  where avg_diff - gmv_stddev <= 0
) t1
left join
(
  select distinct
    first_cat_id, first_cat_name
  from
    dim.dim_vova_category dc
) t2
on t1.first_cat_id = t2.first_cat_id
where t1.rank = 1
;

create table if not exists tmp.tmp_vova_goods_reach_norm_detail_${table_suffix} as
  select /*+ REPARTITION(1) */
    dg.first_cat_id,
    regexp_replace(dg.first_cat_name, '\'', ' ') first_cat_name,
    dg.goods_id,
    t3.group_id,
    dg.mct_id,
    dm.spsor_name,
    t4.group_id mct_group_id
  from
    dwb.dwb_vova_royalty_norm t1 -- 阈值
  left join
    tmp.tmp_vova_first_cat_group_gmv_${table_suffix} t2
  on t1.first_cat_id = t2.first_cat_id
  left join
    ods_vova_vbts.ods_vova_rec_gid_pic_similar t3
  on t2.group_id = t3.group_id
  left join
    dim.dim_vova_goods dg
  on t3.goods_id = dg.goods_id
  left join
    dim.dim_vova_merchant dm
  on dg.mct_id = dm.mct_id
  left join
    ods_vova_vtr.ods_vova_risk_merchant_relation_extra t4
  on dg.mct_id = t4.merchant_id
  where t2.gmv >= t1.month_sale_threshold -- 大于阈值的商品组
    and dg.brand_id = 0 -- 非brand商品
    and to_date(dm.first_publish_time) >= date_sub('${cur_date}', 90) -- ID对应的店铺为近3个月的新激活店铺（店铺首次上架商品的时间）
    and to_date(dm.first_publish_time) <= '${cur_date}'
;

-- 店铺近3个月与其他店铺无关联
create table if not exists tmp.tmp_vova_rela_mct_${table_suffix} as
select /*+ REPARTITION(1) */
  mct_id,
  count(distinct vaild_group_id) vaild_group_cnt -- 店铺所在店铺组整组都是近三个月激活的店铺组数， 等于0 时满足无关联
from
(
  select
    mct_id,
    count(distinct rela_mct_id) rela_mct_cnt,
    count(distinct is_new) is_new_cnt,
    if(count(distinct rela_mct_id) > 0 and count(distinct rela_mct_id) = count(distinct is_new), mct_group_id, null) vaild_group_id -- 同一店铺组中的店铺全部是近三个月激活
  from
  (
    select
      t1.mct_id,
      t1.mct_group_id,
      t2.merchant_id rela_mct_id, -- 关联店铺
      dm.first_publish_time, -- 关联店铺第一次发布商品的时间
      if(to_date(dm.first_publish_time) >= date_sub('${cur_date}', '90') and to_date(dm.first_publish_time) <= '${cur_date}', t2.merchant_id, null) is_new -- 是否在近三个月激活
    from
    (
      select distinct -- 过滤处理的 店铺及店铺组
        mct_id, mct_group_id
      from
        tmp.tmp_vova_goods_reach_norm_detail_${table_suffix}
    ) t1
    left join
      ods_vova_vtr.ods_vova_risk_merchant_relation_extra t2
    on t1.mct_group_id = t2.group_id and t1.mct_id != t2.merchant_id -- 店铺组下的其他店铺
    left join
      dim.dim_vova_merchant dm
    on t2.merchant_id = dm.mct_id
    where t2.merchant_id is not null
      and t2.rule_info != 'IP关联'
  ) tmp1
  group by mct_id, mct_group_id
) t1
where rela_mct_cnt > 0
group by mct_id
-- having vaild_group_cnt = 0
;

insert overwrite table dwb.dwb_vova_goods_reach_norm_detail partition(pt='${cur_month}')
select /*+ REPARTITION(1) */
  distinct
  t1.first_cat_id,
  t1.first_cat_name,
  t1.goods_id,
  t1.group_id,
  t1.spsor_name
from
  tmp.tmp_vova_goods_reach_norm_detail_${table_suffix} t1
left join
  tmp.tmp_vova_rela_mct_${table_suffix} t2
on t1.mct_id = t2.mct_id
left join
(
  select
  *
  from
    dwb.dwb_vova_goods_group_inc
  where pt < '${cur_month}'
) t3
on t1.group_id = t3.group_id
where t2.mct_id is not null and t2.vaild_group_cnt = 0 -- 店铺近3个月与其他店铺无关联（取关联店铺的创建时间）
  and t3.group_id is null -- 去掉本月之前已经添加过的有效商品组
;

insert overwrite table dwb.dwb_vova_goods_group_inc partition(pt='${cur_month}')
select /*+ REPARTITION(1) */
  distinct
  first_cat_id,
  group_id
from
  dwb.dwb_vova_goods_reach_norm_detail
where pt='${cur_month}'
;

-- 招商提成报表-提成汇总
insert overwrite table dwb.dwb_vova_commission partition(pt='${cur_month}')
select /*+ REPARTITION(1) */
  first_cat_id,
  first_cat_name,
  spsor_name,
  count(distinct goods_id) goods_cnt,
  count(distinct goods_id) * 200 commission
from
  dwb.dwb_vova_goods_reach_norm_detail
where pt ='${cur_month}'
group by first_cat_id, first_cat_name, spsor_name
;

drop table if exists tmp.tmp_vova_first_cat_group_gmv_${table_suffix};
drop table if exists tmp.tmp_vova_goods_reach_norm_detail_${table_suffix};
drop table if exists tmp.tmp_vova_rela_mct_${table_suffix};
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
