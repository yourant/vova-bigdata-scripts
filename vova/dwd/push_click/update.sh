#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_push_click
###更新
sql="
insert overwrite table dwd.dwd_vova_fact_push_click
select /*+ REPARTITION(20) */
       'vova'                          as datasource,
       vaelmp.uid                                             as buyer_id,
       nvl(regexp_extract(ug.user_tag, 'R_([0-9])', 0), 'NA') as r_tag,
       nvl(regexp_extract(ug.user_tag, 'F_([0-9])', 0), 'NA') as f_tag,
       nvl(regexp_extract(ug.user_tag, 'M_([0-9])', 0), 'NA') as m_tag,
       case
           when ug.user_tag like '%is_new%' then 'new'
           else 'old'
           end                                                as is_new,
       vaelmp.task_id,
       vaelmp.event_time                                      as click_time,
       vaelmp.domain                                          as from_domain,
       vaelmp.platform,
       vaelmp.device_id,
       vaelmp.app_version,
       vaelmp.country                                         as region_code,
       vaelmp.currency                                        as currency_code,
       nvl(vapt2.config_id, 'NA')                             as config_id,
       nvl(vaptc.push_mode_id, 'NA')                          as push_mode_id,
       nvl(vaptc.push_platform_id, 'NA')                      as push_platform_id,
       nvl(vaptc.priority, 'NA')                              as priority,
       nvl(vaptc.message_title, 'NA')                         as message_title,
       nvl(vaptc.message_body, 'NA')                          as message_body,
       nvl(vaptc.target_link, 'NA')                           as target_link,
       nvl(vaptc.task_type, 'NA')                             as task_type,
       nvl(vaptc.target_type, 'NA')                           as target_type,
       nvl(vaptc.target_tags, 'NA')                           as target_tags,
       nvl(vapt2.utc, 'NA')                                   as time_zone,
       nvl(vapt2.push_country, 'NA')                          as push_region_code,
       nvl(vapt2.task_status, 'NA')                           as task_status,
       nvl(vapt2.push_time, 'NA')                             as push_time
from ods_vova_vtp.ods_vova_app_event_log_message_push vaelmp
         left join ods_vova_vtp.ods_vova_app_push_task vapt2 on vapt2.id = vaelmp.task_id
         left join ods_vova_vtp.ods_vova_app_push_task_config vaptc on vaptc.id = vapt2.config_id
         left join (select vut.user_id                            as buyer_id,
                           concat_ws(',', collect_set(vapt.code)) as user_tag
                    from ods_vova_vtp.ods_vova_user_tags vut
                             inner join ods_vova_vtp.ods_vova_app_push_tag vapt on vut.tag_id = vapt.id
                    group by vut.user_id) as ug on ug.buyer_id = vaelmp.uid
union all
select 'airyclub'                          as datasource,
       vaelmp.uid                                             as buyer_id,
       nvl(regexp_extract(ug.user_tag, 'R_([0-9])', 0), 'NA') as r_tag,
       nvl(regexp_extract(ug.user_tag, 'F_([0-9])', 0), 'NA') as f_tag,
       nvl(regexp_extract(ug.user_tag, 'M_([0-9])', 0), 'NA') as m_tag,
       case
           when ug.user_tag like '%is_new%' then 'new'
           else 'old'
           end                                                as is_new,
       vaelmp.task_id,
       vaelmp.event_time                                      as click_time,
       vaelmp.domain                                          as from_domain,
       vaelmp.platform,
       vaelmp.device_id,
       vaelmp.app_version,
       vaelmp.country                                         as region_code,
       vaelmp.currency                                        as currency_code,
       nvl(vapt2.config_id, 'NA')                             as config_id,
       nvl(vaptc.push_mode_id, 'NA')                          as push_mode_id,
       nvl(vaptc.push_platform_id, 'NA')                      as push_platform_id,
       nvl(vaptc.priority, 'NA')                              as priority,
       nvl(vaptc.message_title, 'NA')                         as message_title,
       nvl(vaptc.message_body, 'NA')                          as message_body,
       nvl(vaptc.target_link, 'NA')                           as target_link,
       nvl(vaptc.task_type, 'NA')                             as task_type,
       nvl(vaptc.target_type, 'NA')                           as target_type,
       nvl(vaptc.target_tags, 'NA')                           as target_tags,
       nvl(vapt2.utc, 'NA')                                   as time_zone,
       nvl(vapt2.push_country, 'NA')                          as push_region_code,
       nvl(vapt2.task_status, 'NA')                           as task_status,
       nvl(vapt2.push_time, 'NA')                             as push_time
from ods_vova_vtp.ods_vova_ac_app_event_log_message_push vaelmp
         left join ods_vova_vtp.ods_vova_app_push_task vapt2 on vapt2.id = vaelmp.task_id
         left join ods_vova_vtp.ods_vova_app_push_task_config vaptc on vaptc.id = vapt2.config_id
         left join (select vut.user_id                            as buyer_id,
                           concat_ws(',', collect_set(vapt.code)) as user_tag
                    from ods_vova_vtp.ods_vova_user_tags vut
                             inner join ods_vova_vtp.ods_vova_app_push_tag vapt on vut.tag_id = vapt.id
                    group by vut.user_id) as ug on ug.buyer_id = vaelmp.uid;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
#hive -e "$sql"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=dwd_vova_fact_push_click" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=40" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


