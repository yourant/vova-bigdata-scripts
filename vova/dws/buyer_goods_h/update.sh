#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# cur_hour=`date +%H`
# if [ ${cur_hour} -eq 00 ]; then
#   echo "0 点不执行！"
#   exit 0
# fi
# echo "cur_hour: ${cur_hour}"

#指定日期和引擎
cur_date=$1
# 每小时执行一次，每次执行当天全部时间
#默认日期为今天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 hour" +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="dws_vova_buyer_goods_behave_h_req9592_gongrui_chenkai"

# msck repair table dwd.dwd_vova_log_goods_impression_arc;
# msck repair table dwd.dwd_vova_log_impressions_arc;
# msck repair table dwd.dwd_vova_log_goods_click_arc;
# msck repair table dwd.dwd_vova_log_click_arc;
# msck repair table dwd.dwd_vova_log_common_click_arc;
# msck repair table dwd.dwd_vova_log_data_arc;

###逻辑sql
sql="
ALTER TABLE dws.dws_vova_buyer_goods_behave_h DROP if exists partition(pt = '$(date -d "${cur_date:0:10} -32day" +%Y-%m-%d)');

insert overwrite table dws.dws_vova_buyer_goods_behave_h partition(pt='${cur_date}')
select /*+ REPARTITION(5) */
  tmp_expre.buyer_id,
  dg.goods_id as gs_id,
  dg.cat_id,
  dg.first_cat_id ,
  dg.second_cat_id,
  dg.third_cat_id ,
  dg.brand_id     ,
  nvl(tmp_expre.expre_cnt, 0) impression_cnt,
  nvl(tmp_clk.clk_cnt, 0),
  nvl(tmp_add_cat.collect_cnt, 0),
  nvl(tmp_add_cat.add_cat_cnt, 0),
  nvl(tmp_click_order.ord_cnt, 0) ord_cnt
from
(
  select
    gi.buyer_id,
    gi.virtual_goods_id as vir_gs_id,
    count(*) as expre_cnt
  from
    -- dwd.dwd_vova_log_goods_impression_arc
  (
    select
      buyer_id,
      virtual_goods_id,
      platform
    from
      dwd.dwd_vova_log_goods_impression_arc
    WHERE (pt='${cur_date}'and date(collector_ts)='${cur_date}') or (pt=date_sub('${cur_date}',1) and hour ='23' and date(collector_ts)='${cur_date}') or (pt=date_add('${cur_date}',1) and hour ='00' and date(collector_ts)='${cur_date}')
      and datasource = 'vova'
    union all 
    select
      buyer_id,
      cast(element_id as bigint) virtual_goods_id,
      platform
    from
      dwd.dwd_vova_log_impressions_arc
    WHERE ((pt='${cur_date}'and date(collector_ts)='${cur_date}' ) or (pt=date_sub('${cur_date}',1) and hour ='23' and date(collector_ts)='${cur_date}') or (pt=date_add('${cur_date}',1) and hour ='00' and date(collector_ts)='${cur_date}')) and event_type='goods'
      and datasource = 'vova'
    
  ) gi
  where platform ='mob'
  group by gi.buyer_id,gi.virtual_goods_id
) tmp_expre
left join
  -- 点击数数据
(
  select
    gc.buyer_id,
    gc.virtual_goods_id as vir_gs_id,
    count(*) as clk_cnt,
    count(*) as clk_valid_cnt
  from
  -- dwd.dwd_vova_log_goods_click_arc
  (
    select
      buyer_id,
      virtual_goods_id
    FROM
      dwd.dwd_vova_log_goods_click_arc
    WHERE pt='${cur_date}'
      and datasource = 'vova'
    union all
    select
      buyer_id,
      cast(element_id as bigint) virtual_goods_id
    FROM
      dwd.dwd_vova_log_click_arc
    WHERE ((pt='${cur_date}'and date(collector_ts)='${cur_date}') or (pt=date_sub('${cur_date}',1) and hour ='23' and date(collector_ts)='${cur_date}') or (pt=date_add('${cur_date}',1) and hour ='00' and date(collector_ts)='${cur_date}')) and event_type='goods'
      and datasource = 'vova'
  ) gc
  group by gc.buyer_id,gc.virtual_goods_id
) tmp_clk
on tmp_clk.buyer_id = tmp_expre.buyer_id and tmp_clk.vir_gs_id = tmp_expre.vir_gs_id
left join
  -- 加车收藏数据
(
  select
    cc.buyer_id,
    cast(cc.element_id as bigint) as vir_gs_id,
    count(*) as clk_cnt,
    sum(if(cc.element_name ='pdAddToWishlistClick',1,0)) as collect_cnt,
    sum(if(cc.element_name ='pdAddToCartSuccess',1,0)) as add_cat_cnt
  from
  -- dwd.dwd_vova_log_common_click_arc
  (
    SELECT
      platform,
      buyer_id,
      element_name,
      element_id,
      datasource
    FROM dwd.dwd_vova_log_common_click_arc
    WHERE (pt='${cur_date}'and date(collector_ts)='${cur_date}' ) or (pt=date_sub('${cur_date}',1) and hour ='23' and date(collector_ts)='${cur_date}') or (pt=date_add('${cur_date}',1) and hour ='00' and date(collector_ts)='${cur_date}')
      and datasource = 'vova'
    union all
    SELECT
      platform,
      buyer_id,
      element_name,
      element_id,
      datasource
    FROM dwd.dwd_vova_log_click_arc
    WHERE (pt='${cur_date}'and date(collector_ts)='${cur_date}' ) or (pt=date_sub('${cur_date}',1) and hour ='23' and date(collector_ts)='${cur_date}') or (pt=date_add('${cur_date}',1) and hour ='00' and date(collector_ts)='${cur_date}') and event_type='normal'
      and datasource = 'vova'
    union all
    SELECT
      platform,
      buyer_id,
      element_name,
      element_id,
      datasource
    FROM dwd.dwd_vova_log_data_arc
    WHERE (pt='${cur_date}'and date(collector_ts)='${cur_date}' ) or (pt=date_sub('${cur_date}',1) and hour ='23' and date(collector_ts)='${cur_date}') or (pt=date_add('${cur_date}',1) and hour ='00' and date(collector_ts)='${cur_date}') and element_name='pdAddToCartSuccess'
      and datasource = 'vova'
    
  ) cc
  where platform ='mob'
  group by cc.buyer_id,cc.element_id
) tmp_add_cat
on tmp_expre.buyer_id = tmp_add_cat.buyer_id and tmp_expre.vir_gs_id = tmp_add_cat.vir_gs_id
left join
  -- 下单点击
(
  select
    buyer_id,
    virtual_goods_id,
    count(*) ord_cnt
  from
  (
    select
      buyer_id,
      page_code,
      element_name,
      element_id,
      lv.col3 virtual_goods_id
    from
    (
      select
        buyer_id,
        page_code,
        element_name,
        element_id,
        get_json_object(extra, '$.goods_id') virtual_goods_id_list
      from
        dwd.dwd_vova_log_click_arc
      where pt='${cur_date}'
        and page_code = 'checkout_new'
        and element_name = 'checkout_place_order'
        and event_type='normal'
    ) t1
    lateral view explode(split(virtual_goods_id_list, ',')) lv as col3
  )
  group by buyer_id, virtual_goods_id
) tmp_click_order
on tmp_expre.buyer_id = tmp_click_order.buyer_id and tmp_expre.vir_gs_id = tmp_click_order.virtual_goods_id
inner join
  dim.dim_vova_goods dg
on tmp_expre.vir_gs_id = dg.virtual_goods_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


