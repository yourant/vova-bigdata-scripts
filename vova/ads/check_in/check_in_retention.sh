#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
insert overwrite table tmp.tmp_check_in_new_device
select region_code,
      gmv_stage,
      device_id,
      pt
from (
        select v.pt,
               v.device_id,
               nvl(d.region_code, 'NA') as region_code,
               nvl(y.gmv_stage, 5)      as gmv_stage,
               row_number()                over(partition by v.pt,v.device_id order by v.buyer_id desc) rk
        from dwd.dwd_vova_log_screen_view v
                 inner join dim.dim_vova_devices d
                            on d.device_id = v.device_id
                                and d.datasource = v.datasource
                                and to_date(d.activate_time) = v.pt
                 left join (select *
                            from ads.ads_vova_buyer_portrait_feature
                            where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                           on y.buyer_id = v.buyer_id
                               and v.datasource = y.datasource
        where v.pt >= date_sub('${cur_date}', 30)
          and v.pt <= '${cur_date}'
          and v.datasource = 'vova'
          and v.page_code = 'coins_rewards'
    ) tmp
where rk = 1
;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_check_in_retention PARTITION (pt)
select if(r1.region_code is null, 'all', r1.region_code)             as region_code,
       if(r1.gmv_stage is null, 'all', cast(r1.gmv_stage as string)) as gmv_stage,
       r1.uv_1                                                 as check_in_uv,
       nvl(r2.uv_2 / r1.uv_1, 0) * 100                         as retention_2,
       nvl(r3.uv_3 / r1.uv_1, 0) * 100                         as retention_3,
       nvl(r7.uv_7 / r1.uv_1, 0) * 100                         as retention_7,
       nvl(r14.uv_14 / r1.uv_1, 0) * 100                       as retention_14,
       nvl(r30.uv_30 / r1.uv_1, 0) * 100                       as retention_30,
       r1.pt as pt
from (
         select region_code,
                gmv_stage,
                count(distinct device_id) as uv_1,
                pt
         from tmp.tmp_check_in_new_device
         group by region_code,
                  gmv_stage,
                  pt grouping sets (
            (pt),
            (pt, region_code),
            (pt,gmv_stage),
            (pt,region_code,gmv_stage)
            )
     ) r1
         left join (
    select t.region_code,
           t.gmv_stage,
           count(distinct t.device_id) as uv_2,
           t.pt
    from tmp.tmp_check_in_new_device t
             inner join (
        select distinct pt, device_id
        from dwd.dwd_vova_log_screen_view
        where page_code = 'coins_rewards'
          and datasource = 'vova'
    ) b
                        on b.device_id = t.device_id
                            and b.pt = date_add(t.pt, 1)
    group by t.region_code,
             t.gmv_stage,
             t.pt grouping sets (
            (t.pt),
            (t.pt, t.region_code),
            (t.pt,t.gmv_stage),
            (t.pt,t.region_code,t.gmv_stage)
            )
) r2
                   on nvl(r2.region_code, '00') = nvl(r1.region_code, '00')
                       and nvl(r2.gmv_stage, 6) = nvl(r1.gmv_stage, 6)
                       and r1.pt = r2.pt
         left join (
    select t.region_code,
           t.gmv_stage,
           count(distinct t.device_id) as uv_3,
           t.pt
    from tmp.tmp_check_in_new_device t
             inner join (
        select distinct pt, device_id
        from dwd.dwd_vova_log_screen_view
        where page_code = 'coins_rewards'
          and datasource = 'vova'
    ) b
                        on b.device_id = t.device_id
                            and b.pt = date_add(t.pt, 2)
    group by t.region_code,
             t.gmv_stage,
             t.pt grouping sets (
            (t.pt),
            (t.pt, t.region_code),
            (t.pt,t.gmv_stage),
            (t.pt,t.region_code,t.gmv_stage)
            )
) r3
                   on nvl(r3.region_code, '00') = nvl(r1.region_code, '00')
                       and nvl(r3.gmv_stage, 6) = nvl(r1.gmv_stage, 6)
                       and r3.pt = r1.pt
         left join (
    select t.region_code,
           t.gmv_stage,
           count(distinct t.device_id) as uv_7,
           t.pt
    from tmp.tmp_check_in_new_device t
             inner join (
        select distinct pt, device_id
        from dwd.dwd_vova_log_screen_view
        where page_code = 'coins_rewards'
          and datasource = 'vova'
    ) b
                        on b.device_id = t.device_id
                            and b.pt = date_add(t.pt, 6)
    group by t.region_code,
             t.gmv_stage,
             t.pt grouping sets (
            (t.pt),
            (t.pt, t.region_code),
            (t.pt,t.gmv_stage),
            (t.pt,t.region_code,t.gmv_stage)
            )
) r7
                   on nvl(r7.region_code, '00') = nvl(r1.region_code, '00')
                       and nvl(r7.gmv_stage, 6) = nvl(r1.gmv_stage, 6)
                       and r7.pt = r1.pt
         left join (
    select t.region_code,
           t.gmv_stage,
           count(distinct t.device_id) as uv_14,
           t.pt
    from tmp.tmp_check_in_new_device t
             inner join (
        select distinct pt, device_id
        from dwd.dwd_vova_log_screen_view
        where page_code = 'coins_rewards'
          and datasource = 'vova'
    ) b
                        on b.device_id = t.device_id
                            and b.pt = date_add(t.pt, 13)
    group by t.region_code,
             t.gmv_stage,
             t.pt grouping sets (
            (t.pt),
            (t.pt, t.region_code),
            (t.pt,t.gmv_stage),
            (t.pt,t.region_code,t.gmv_stage)
            )
) r14
                   on nvl(r14.region_code, '00') = nvl(r1.region_code, '00')
                       and nvl(r14.gmv_stage, 6) = nvl(r1.gmv_stage, 6)
                       and r14.pt = r1.pt
         left join (
    select t.region_code,
           t.gmv_stage,
           count(distinct t.device_id) as uv_30,
           t.pt
    from tmp.tmp_check_in_new_device t
             inner join (
        select distinct pt, device_id
        from dwd.dwd_vova_log_screen_view
        where page_code = 'coins_rewards'
          and datasource = 'vova'
    ) b
                        on b.device_id = t.device_id
                            and b.pt = date_add(t.pt, 29)
    group by t.region_code,
             t.gmv_stage,
             t.pt grouping sets (
            (t.pt),
            (t.pt, t.region_code),
            (t.pt,t.gmv_stage),
            (t.pt,t.region_code,t.gmv_stage)
            )
) r30
                   on nvl(r30.region_code, '00') = nvl(r1.region_code, '00')
                       and nvl(r30.gmv_stage, 6) = nvl(r1.gmv_stage, 6)
                       and r30.pt = r1.pt
"

spark-sql \
--conf "spark.app.name=ads_vova_check_in_retention_huachen" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi