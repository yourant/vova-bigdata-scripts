#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
# 表1 每天一次
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="dwb_vova_active_user_form_d_req8931_chenkai_${cur_date}"

#
sql="
insert overwrite table dwb.dwb_vova_active_user_form_d partition(pt ='${cur_date}')
select /*+ REPARTITION(1) */
  region_code,
  main_channel,
  platform,
  activate_month,
  uv,
  pay_uv
from
(
  select
    nvl(region_code, 'all') region_code,
    nvl(main_channel, 'all') main_channel,
    nvl(platform, 'all') platform,
    nvl(activate_month, 'all') activate_month,
    count(distinct activate_device_id) uv,
    count(distinct pay_device_id) pay_uv
  from
  (
    select
      if(dd.region_code is null or dd.region_code = '', 'others', dd.region_code) region_code, -- 国家
      case when dd.main_channel = 'googleadwords_int' then 'google'
        when dd.main_channel = 'Facebook Ads' then 'facebook'
        when dd.main_channel in ('organic','Organic','youtube_organic') then 'organic'
        when dd.main_channel is null or dd.main_channel = '' then 'NA'
        else 'others' end main_channel, -- 主渠道
      dd.platform platform, -- 平台
      if(activate_time <'2015-01-01', 'others', trunc(activate_time, 'MM'))  activate_month, -- 激活月份
      sv.device_id activate_device_id, -- 活跃用户
      fp.device_id pay_device_id -- 支付用户
    from
      dim.dim_vova_devices dd
    left join
      dwd.dwd_vova_log_screen_view sv
    on dd.device_id = sv.device_id
    left join
    (
      select *
      from
        dwd.dwd_vova_fact_pay
      where to_date(pay_time) = '${cur_date}'
    ) fp
    on dd.device_id = fp.device_id
    where dd.datasource = 'vova'
      and dd.platform in ('ios','android')
      and sv.pt = '${cur_date}'
      and sv.dp = 'vova'
  )
  group by cube(region_code, main_channel, platform, activate_month)
)
where region_code in ('all','GB','FR','DE','IT','ES','US')
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
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