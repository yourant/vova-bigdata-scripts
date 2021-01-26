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

job_name="rpt_bonus_card_req6571_chenkai_table1"

###逻辑sql
sql="
insert overwrite table rpt.rpt_bonus_card_conversion partition(pt='${cur_date}')
select
/*+ REPARTITION(1) */
  'vova' datasource,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  count(distinct(buyer_id)) dau, -- 非月卡DAU
  count(distinct(vouchercard_hp_unpay_buyer_id))   vouchercard_hp_unpay_uv, -- 未购卡会场页面UV（非pending）
  count(distinct(payment_bonus_card_buyer_id)) payment_bonus_card_uv, --   月卡支付页面UV
  count(distinct(paid_buyer_id)) paid_uv, -- 开卡成功UV
  count(distinct(pending_buyer_id)) pending_uv -- 开卡pendingUV
from
(
  select
  /*+ REPARTITION(100) */
    nvl(db.region_code, 'NA') region_code,
    nvl(tmp1.os_type, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel, -- 渠道
    CASE WHEN datediff(tmp1.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(tmp1.pt,dd.activate_time)>=1 and datediff(tmp1.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(tmp1.pt,dd.activate_time)>=7 and datediff(tmp1.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    dd.device_id device_id,
    if(tmp1.status is null or tmp1.status != 'paid' or from_unixtime(tmp1.start_time, 'yyyy-MM-dd')='${cur_date}', tmp1.buyer_id, null) buyer_id, --   非月卡DAU
    if(tmp1.page_code = 'vouchercard_hp_unpay' and (tmp1.status is null or tmp1.status != 'pending'), tmp1.buyer_id, null) vouchercard_hp_unpay_buyer_id, -- 未购卡会场页面UV(非pending)
    if(tmp1.status is not null and create_time='${cur_date}', tmp1.buyer_id, null)   payment_bonus_card_buyer_id, -- 月卡支付页面UV
    if(tmp1.status = 'paid' and create_time = '${cur_date}' , tmp1.buyer_id, null) paid_buyer_id, -- 开卡成功UV
    if(tmp1.status = 'pending' and create_time = '${cur_date}', tmp1.buyer_id, null) pending_buyer_id -- 开卡pendingUV
  from
  (
    select
    /*+ REPARTITION(200) */
      COALESCE(flsv.buyer_id, vbc.user_id) buyer_id,
      COALESCE(flsv.pt, from_unixtime(vbc.start_time, 'yyyy-MM-dd')) pt,
      flsv.page_code page_code,
      flsv.os_type os_type,
      vbc.status status,
      vbc.start_time start_time,
      vbc.create_time create_time
    from
    (
      select
        tmp_card.user_id user_id,
        if(tmp_log.new_status is not null, tmp_log.new_status, tmp_card.status) status,
        if(tmp_log.new_status ='paid', tmp_log.update_time, tmp_card.start_time) start_time,
        if(tmp_log.update_time is not null, from_unixtime(tmp_log.update_time, 'yyyy-MM-dd'), to_date(tmp_card.create_time, 'yyyy-MM-dd')) create_time
      from
      (
        select
          *
        from
          ods.vova_bonus_card
        where status = 'pending'
          or (status='paid' and from_unixtime(end_time, 'yyyy-MM-dd')>='${cur_date}')
          or (status='unpaid' and to_date(update_time, 'yyyy-MM-dd') = '${cur_date}')
      ) tmp_card
      left join
      (
        select
          *
        from
        (
          select *,
            row_number() over(partition by user_id order by update_time desc) row
          from
            dwd.fact_log_bonus_card
          where pt = '${cur_date}'
        ) where row = 1
      ) tmp_log
      on tmp_card.user_id = tmp_log.user_id
    ) vbc
    full outer join
    (
      select * from
      dwd.fact_log_screen_view
      where pt = '${cur_date}'
        and platform = 'mob'
        and datasource = 'vova'
        and email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
    ) flsv
    on flsv.buyer_id = vbc.user_id
  ) tmp1
  left join
    dwd.dim_buyers db
  on tmp1.buyer_id = db.buyer_id
  left join
    dwd.dim_devices dd
  on db.datasource = dd.datasource and db.current_device_id = dd.device_id
  left join
    ads.ads_buyer_gmv_stage_3m abgs
  on tmp1.buyer_id = abgs.buyer_id
)
group by cube(region_code, os_type, main_channel, is_new, gmv_stage)
  having region_code in ('FR','DE','IT','ES','GB','US','NA','PL','CH','BE','TW','RU','RO','NL','IN', 'all')
;

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
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
