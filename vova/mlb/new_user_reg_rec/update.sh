#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="mlb_vova_new_user_reg_rec_req7832_chenkai"

###逻辑sql
sql="
INSERT overwrite TABLE mlb.mlb_vova_rec_new_user_reg_d PARTITION (pt = '${cur_date}')
select
  /*+ REPARTITION(20) */
  region_id             ,
  cat_id                ,
  goods_id              ,
  clk_cnt               ,
  expre_cnt             ,
  impression_uv         ,
  gmv                   ,
  gcr                   ,
  sales_vol             ,
  order_goods_cnt       ,
  refund_order_goods_cnt,
  collect_cnt           ,
  add_cat_cnt           ,
  refund_rate           ,
  comment_cnt           ,
  good_comment_cnt      ,
  bad_comment_cnt       ,
  rank_index            ,
  click_uv
from
(
  select
    region_id,
    dg.cat_id cat_id,
    t2.goods_id goods_id,
    clk_cnt, -- 点击量
    expre_cnt, -- 曝光量
    impression_uv, -- 曝光UV
    gmv, -- GMV
    round(nvl(gmv / click_uv * clk_cnt / expre_cnt * 10000, 0), 4) gcr, -- GCR
    sales_vol, -- 销量
    order_goods_cnt, -- 子订单量
    refund_order_goods_cnt, -- 退款子订单量
    collect_cnt,
    add_cat_cnt,
    round(nvl(refund_order_goods_cnt / order_goods_cnt, 0), 4) refund_rate, -- 退款率
    comment_cnt, -- 评价数
    good_comment_cnt, -- 好评量
    bad_comment_cnt, -- 差评量
    row_number() over(partition by region_id order by gmv / click_uv * clk_cnt / expre_cnt * 10000 desc, gmv desc, sales_vol desc) rank_index, -- GCR倒排索引
    click_uv
  from
  (
  select
    goods_id goods_id,
    region_id region_id,
    sum(clk_cnt) clk_cnt,
    sum(expre_cnt) expre_cnt,
    count(distinct click_buyer_id) click_uv,
    count(distinct impression_buyer_id) impression_uv,
    sum(gmv) gmv,
    sum(sales_vol) sales_vol,
    count(distinct order_goods_id) order_goods_cnt,
    count(distinct refund_order_goods_id) refund_order_goods_cnt,
    sum(collect_cnt) collect_cnt,
    sum(add_cat_cnt) add_cat_cnt,
    count(distinct comment_id) comment_cnt,
    count(distinct good_comment_id) good_comment_cnt,
    count(distinct bad_comment_id) bad_comment_cnt
  from
  (
  select
    t1.goods_id goods_id,
    nvl(db.user_age_group, 'unknown') user_age_group,
    nvl(db.region_id, 'unknown') region_id,
    nvl(db.platform, 'unknown') platform,
    if(db.gender = '' or db.gender is null, 'unknown', db.gender) gender,
    t1.buyer_id,
    clk_cnt,
    expre_cnt,
    click_buyer_id,
    impression_buyer_id,
    collect_cnt,
    add_cat_cnt,
    gmv,
    sales_vol,
    order_goods_id,
    refund_order_goods_id,
    comment_id,
    good_comment_id,
    bad_comment_id
  from
  (
    select
      gs_id goods_id,
      buyer_id,
      clk_cnt,
      expre_cnt,
      if(clk_cnt > 0, buyer_id, null) click_buyer_id,
      if(expre_cnt > 0, buyer_id, null) impression_buyer_id,
      collect_cnt,
      add_cat_cnt,
      gmv gmv,
      null sales_vol,
      null order_goods_id,
      null refund_order_goods_id,
      null comment_id,
      null good_comment_id,
      null bad_comment_id
    from
      dws.dws_vova_buyer_goods_behave
    where pt >= date_sub('${cur_date}', 14) and pt <= '${cur_date}'

    union all
    select
      goods_id,
      buyer_id,
      null clk_cnt,
      null expre_cnt,
      null click_buyer_id,
      null impression_buyer_id,
      null collect_cnt,
      null add_cat_cnt,
      null gmv,
      null sales_vol,
      null order_goods_id,
      null refund_order_goods_id,
      comment_id comment_id,
      IF (rating <= 2, comment_id, null) good_comment_id,
      IF (rating = 5, comment_id, null) bad_comment_id
    from
      dwd.dwd_vova_fact_comment
    where to_date(post_time) >= date_sub('${cur_date}', 14)
      AND to_date(post_time) <= '${cur_date}'
      and datasource = 'vova'

    union all
    select
      dog.goods_id,
      dog.buyer_id,
      null clk_cnt,
      null expre_cnt,
      null click_buyer_id,
      null impression_buyer_id,
      null collect_cnt,
      null add_cat_cnt,
      null gmv,
      null sales_vol,
      null order_goods_id,
      fr.order_goods_id refund_order_goods_id,
      null comment_id,
      null good_comment_id,
      null bad_comment_id
    from
      dwd.dwd_vova_fact_refund fr
    left join
      dim.dim_vova_order_goods dog
    on fr.order_goods_id = dog.order_goods_id
    where to_date(create_time) >= date_sub('${cur_date}', 14)
      and to_date(create_time) <= '${cur_date}'
      and fr.datasource = 'vova' and dog.datasource = 'vova'

    union all
    select
      goods_id,
      buyer_id,
      null clk_cnt,
      null expre_cnt,
      null click_buyer_id,
      null impression_buyer_id,
      null collect_cnt,
      null add_cat_cnt,
      null gmv,
      goods_number sales_vol,
      order_goods_id order_goods_id,
      null refund_order_goods_id,
      null comment_id,
      null good_comment_id,
      null bad_comment_id
    from
      dwd.dwd_vova_fact_pay
    where to_date(order_time) >= date_sub('${cur_date}', 14)
      and to_date(order_time) <= '${cur_date}' and datasource = 'vova'
  ) t1
  left join
    dim.dim_vova_buyers db
  on t1.buyer_id = db.buyer_id
  )
  group by goods_id, region_id
  ) t2
  left join
    dim.dim_vova_goods dg
  on t2.goods_id = dg.goods_id
  where dg.datasource = 'vova' and dg.is_on_sale = 1
)
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 12G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
