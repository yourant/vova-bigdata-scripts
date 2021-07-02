#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
echo "${cur_date}"
job_name="dwb_vova_devices_tag_req_chenkai_${cur_date}"

###逻辑sql
#优惠券使用
sql="
insert overwrite table dwb.dwb_vova_devices_tag PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
       datasource                  as datasource,
       event_date                  as event_date,
       region_code                 as region_code,
       main_channel                as main_channel,
       sum(first_pay_uv)           as first_pay_uv,
       sum(pay_uv)                 as pay_uv,
       sum(dau)                    as dau,
       sum(dau_no_pay)             as dau_no_pay,
       sum(dau_continue_1d)        as dau_continue_1d,
       sum(dau_continue_1d_no_pay) as dau_continue_1d_no_pay,
       sum(loss_user_1)            as loss_user_1,
       sum(loss_user_1_pay)        as loss_user_1_pay,
       sum(loss_user_2)            as loss_user_2,
       sum(loss_user_2_pay)        as loss_user_2_pay,
       sum(dau_r1)                 as dau_r1,
       sum(dau_r1_continue_1d)     as dau_r1_continue_1d,
       sum(dau_r2)                 as dau_r2,
       sum(dau_r2_continue_1d)     as dau_r2_continue_1d,
       sum(dau_r3)                 as dau_r3,
       sum(dau_r3_continue_1d)     as dau_r3_continue_1d,
       sum(dau_r4)                 as dau_r4,
       sum(dau_r4_continue_1d)     as dau_r4_continue_1d,
       sum(dau_m1)                 as dau_m1,
       sum(dau_m1_continue_1d)     as dau_m1_continue_1d,
       sum(dau_m2)                 as dau_m2,
       sum(dau_m2_continue_1d)     as dau_m2_continue_1d,
       sum(dau_m3)                 as dau_m3,
       sum(dau_m3_continue_1d)     as dau_m3_continue_1d,
       sum(dau_m4)                 as dau_m4,
       sum(dau_m4_continue_1d)     as dau_m4_continue_1d,
       sum(dau_F1)                 as dau_F1,
       sum(dau_F1_continue_1d)     as dau_F1_continue_1d,
       sum(dau_F2)                 as dau_F2,
       sum(dau_F2_continue_1d)     as dau_F2_continue_1d,
       sum(dau_F3)                 as dau_F3,
       sum(dau_F3_continue_1d)     as dau_F3_continue_1d,
       sum(dau_F4)                 as dau_F4,
       sum(dau_F4_continue_1d)     as dau_F4_continue_1d
from (
         select nvl(nvl(su.datasource, 'NA'), 'all')                            as datasource,
                '${cur_date}'                                                   as event_date,
                nvl(nvl(su.region_code, 'NA'), 'all')                           as region_code,
                nvl(nvl(dev.main_channel, 'NA'), 'all')                         as main_channel,
                sum(if(date(dev1.first_pay_time) = '${cur_date}', 1, 0))        as first_pay_uv,
                sum(if(date(dev1.last_pay_time) = '${cur_date}', 1, 0))         as pay_uv,
                sum(if(su.device_id is not null, 1, 0))                         as dau,
                sum(if(dev1.first_pay_time > '2018-03-01', 0, 1))               as dau_no_pay,
                sum(if(dev1.continue_1d = 'Y', 1, 0))                           as dau_continue_1d,
                sum(if(dev1.continue_1d = 'Y' and  dev1.first_pay_time is null, 1, 0))  as dau_continue_1d_no_pay,
                0                                                               as loss_user_1,
                0                                                               as loss_user_1_pay,
                0                                                               as loss_user_2,
                0                                                               as loss_user_2_pay,
                sum(if(dev1.R_tag like 'R_1', 1, 0))                            as dau_r1,
                sum(if(dev1.R_tag like 'R_1' and dev1.continue_1d = 'Y', 1, 0)) as dau_r1_continue_1d,
                sum(if(dev1.R_tag like 'R_2', 1, 0))                            as dau_r2,
                sum(if(dev1.R_tag like 'R_2' and dev1.continue_1d = 'Y', 1, 0)) as dau_r2_continue_1d,
                sum(if(dev1.R_tag like 'R_3', 1, 0))                            as dau_r3,
                sum(if(dev1.R_tag like 'R_3' and dev1.continue_1d = 'Y', 1, 0)) as dau_r3_continue_1d,
                sum(if(dev1.R_tag like 'R_4', 1, 0))                            as dau_r4,
                sum(if(dev1.R_tag like 'R_4' and dev1.continue_1d = 'Y', 1, 0)) as dau_r4_continue_1d,
                sum(if(dev1.M_tag like 'M_1', 1, 0))                            as dau_m1,
                sum(if(dev1.M_tag like 'M_1' and dev1.continue_1d = 'Y', 1, 0)) as dau_m1_continue_1d,
                sum(if(dev1.M_tag like 'M_2', 1, 0))                            as dau_m2,
                sum(if(dev1.M_tag like 'M_2' and dev1.continue_1d = 'Y', 1, 0)) as dau_m2_continue_1d,
                sum(if(dev1.M_tag like 'M_3', 1, 0))                            as dau_m3,
                sum(if(dev1.M_tag like 'M_3' and dev1.continue_1d = 'Y', 1, 0)) as dau_m3_continue_1d,
                sum(if(dev1.M_tag like 'M_4', 1, 0))                            as dau_m4,
                sum(if(dev1.M_tag like 'M_4' and dev1.continue_1d = 'Y', 1, 0)) as dau_m4_continue_1d,
                sum(if(dev1.F_tag like 'F_1', 1, 0))                            as dau_F1,
                sum(if(dev1.F_tag like 'F_1' and dev1.continue_1d = 'Y', 1, 0)) as dau_F1_continue_1d,
                sum(if(dev1.F_tag like 'F_2', 1, 0))                            as dau_F2,
                sum(if(dev1.F_tag like 'F_2' and dev1.continue_1d = 'Y', 1, 0)) as dau_F2_continue_1d,
                sum(if(dev1.F_tag like 'F_3', 1, 0))                            as dau_F3,
                sum(if(dev1.F_tag like 'F_3' and dev1.continue_1d = 'Y', 1, 0)) as dau_F3_continue_1d,
                sum(if(dev1.F_tag like 'F_4', 1, 0))                            as dau_F4,
                sum(if(dev1.F_tag like 'F_4' and dev1.continue_1d = 'Y', 1, 0)) as dau_F4_continue_1d
         from (select distinct device_id,
                               datasource,
                               region_code
               from dwd.dwd_vova_fact_start_up
               where pt = '${cur_date}'
              ) su
                  left join
              (select device_id,
                      datasource,
                      main_channel
               from dim.dim_vova_devices) dev on su.device_id = dev.device_id and su.datasource = dev.datasource
                  left join dws.dws_vova_devices dev1 on su.device_id = dev1.device_id and su.datasource = dev1.datasource
         group by cube (nvl(su.datasource, 'NA'), nvl(su.region_code, 'NA'), nvl(dev.main_channel, 'NA'))
         union
         select nvl(nvl(dev.datasource, 'NA'), 'all')                        as datasource,
                '${cur_date}'                                                as event_date,
                nvl(nvl(dev.region_code, 'NA'), 'all')                       as region_code,
                nvl(nvl(dev.main_channel, 'NA'), 'all')                      as main_channel,
                0                                                            as first_pay_uv,
                0                                                            as pay_uv,
                0                                                            as dau,
                0                                                            as dau_no_pay,
                0                                                            as dau_continue_1d,
                0                                                            as dau_continue_1d_no_pay,
                sum(if(dev1.loss_user like '1%', 1, 0))                      as loss_user_1,
                sum(if(dev1.loss_user like '1%' and dev1.pay_gmv > 0, 1, 0)) as loss_user_1_pay,
                sum(if(dev1.loss_user like '%1', 1, 0))                      as loss_user_2,
                sum(if(dev1.loss_user like '%1' and dev1.pay_gmv > 0, 1, 0)) as loss_user_2_pay,
                0                                                            as dau_r1,
                0                                                            as dau_r1_continue_1d,
                0                                                            as dau_r2,
                0                                                            as dau_r2_continue_1d,
                0                                                            as dau_r3,
                0                                                            as dau_r3_continue_1d,
                0                                                            as dau_r4,
                0                                                            as dau_r4_continue_1d,
                0                                                            as dau_m1,
                0                                                            as dau_m1_continue_1d,
                0                                                            as dau_m2,
                0                                                            as dau_m2_continue_1d,
                0                                                            as dau_m3,
                0                                                            as dau_m3_continue_1d,
                0                                                            as dau_m4,
                0                                                            as dau_m4_continue_1d,
                0                                                            as dau_F1,
                0                                                            as dau_F1_continue_1d,
                0                                                            as dau_F2,
                0                                                            as dau_F2_continue_1d,
                0                                                            as dau_F3,
                0                                                            as dau_F3_continue_1d,
                0                                                            as dau_F4,
                0                                                            as dau_F4_continue_1d
         from (select device_id,
                      datasource,
                      region_code,
                      main_channel
               from dim.dim_vova_devices
              ) dev
                  left join dws.dws_vova_devices dev1 on dev.device_id = dev1.device_id and dev.datasource = dev1.datasource
         group by cube (nvl(dev.datasource, 'NA'), nvl(dev.region_code, 'NA'), nvl(dev.main_channel, 'NA'))
     )
where datasource not in ('all', 'NA')
group by datasource, event_date, region_code, main_channel;
"

#echo "${sql}"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.app.name=${job_name}"  --conf "spark.dynamicAllocation.minExecutors=50" --conf "spark.dynamicAllocation.initialExecutors=50" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# sqoop export \
# -Dorg.apache.sqoop.export.text.dump_data_on_error=true \
# -Dmapreduce.job.queuename=default \
# --connect jdbc:mysql://db-logistics-w.gitvv.com:3306/themis_logistics_report \
# --username vvreport20210517 --password thuy*at1OhG1eiyoh8she \
# --table rpt_devices_tag \
# --update-key datasource,event_date,region_code,main_channel \
# --update-mode allowinsert \
# --hcatalog-database dwb \
# --hcatalog-table dwb_vova_devices_tag \
# --hcatalog-partition-keys pt \
# --hcatalog-partition-values ${cur_date} \
# --fields-terminated-by '\001'
#
# if [ $? -ne 0 ];then
#   exit 1
# fi
