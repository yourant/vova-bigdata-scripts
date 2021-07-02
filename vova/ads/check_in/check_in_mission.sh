#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
--完成APP评价的uv
with tmp_myCoinsTaskreview_app_uv as (
select
activate_range,
region_code,
gmv_stage,
count(distinct device_id) as myCoinsTaskreview_app_uv
from (
select
t.device_id,
case
                    when t.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
from tmp.tmp_myCoinsTaskreview_app t
left join dim.dim_vova_devices dd
                            on t.device_id = dd.device_id
                            and dd.datasource = 'vova'
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t.buyer_id
                            and y.datasource = 'vova'
) tmp
group by
activate_range,
region_code,
gmv_stage
with cube
),
--完成修改头像的uv
tmp_myCoinsTaskupdate_photo_uv as (
select
activate_range,
region_code,
gmv_stage,
count(distinct device_id) as myCoinsTaskupdate_photo_uv
from (
select
t.device_id,
case
                    when t.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
from tmp.tmp_myCoinsTaskupdate_photo t
left join dim.dim_vova_devices dd
                            on t.device_id = dd.device_id
                            and dd.datasource = 'vova'
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t.buyer_id
                            and y.datasource = 'vova'
) tmp
group by
activate_range,
region_code,
gmv_stage
with cube
),
--完成填写地址的uv
tmp_myCoinsTaskcomplete_address_uv as (
select
activate_range,
region_code,
gmv_stage,
count(distinct device_id) as myCoinsTaskcomplete_address_uv
from (
select
t.device_id,
case
                    when t.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
from tmp.tmp_myCoinsTaskcomplete_address t
left join dim.dim_vova_devices dd
                            on t.device_id = dd.device_id
                            and dd.datasource = 'vova'
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t.buyer_id
                            and y.datasource = 'vova'
) tmp
group by
activate_range,
region_code,
gmv_stage
with cube
),
--完成注册任务的uv
tmp_myCoinsTaskregiste_account_uv as (
select
activate_range,
region_code,
gmv_stage,
count(distinct device_id) as myCoinsTaskregiste_account_uv
from (
select
t.device_id,
case
                    when t.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
from tmp.tmp_myCoinsTaskregiste_accountt t
left join dim.dim_vova_devices dd
                            on t.device_id = dd.device_id
                            and dd.datasource = 'vova'
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t.buyer_id
                            and y.datasource = 'vova'
) tmp
group by
activate_range,
region_code,
gmv_stage
with cube
),
tmp_mission_all_uv as (
select activate_range,
       region_code,
       gmv_stage,
       count(distinct device_id) as mission_all_uv
from (
         select t.device_id,
                case
                    when t.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage
         from (
                  select *,
                         row_number() over(partition by device_id order by buyer_id desc) rk
                  from (
                           select *
                           from tmp.tmp_myCoinsTaskregiste_accountt
                           union all
                           select *
                           from tmp.tmp_myCoinsTaskcomplete_address
                           union all
                           select *
                           from tmp.tmp_myCoinsTaskupdate_photo
                           union all
                           select *
                           from tmp.tmp_myCoinsTaskreview_app
                           union all
                           select *
                           from tmp.tmp_myCoinsTaskopen_notification
                           union all
                           select *
                           from tmp.tmp_myCoinsTaskcomplete_shopping
                       )
              ) t
                  left join dim.dim_vova_devices dd
                            on t.device_id = dd.device_id
                            and dd.datasource = 'vova'
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = t.buyer_id
                            and y.datasource = 'vova'
         where t.rk = 1
     ) tmp
group by activate_range,
         region_code,
         gmv_stage with cube
)
insert overwrite table ads.ads_vova_check_in_mission PARTITION (pt='${cur_date}')
select /*+ REPARTITION(1) */
    case
        when activate_range is null then 'all'
        when activate_range = 1 then '新激活'
        when activate_range = 2 then '2-7天'
        when activate_range = 3 then '8-30天'
        when activate_range = 4 then '30天+'
        else '无激活信息'
        end                                                 as activate_range,
    if(region_code is null, 'all', region_code)             as region_code,
    if(gmv_stage is null, 'all', cast(gmv_stage as string)) as gmv_stage,
    sum(check_in_uv)                                        as check_in_uv,
    sum(myCoinsTaskregiste_account_uv)                      as myCoinsTaskregiste_account_uv,
    sum(myCoinsTaskcomplete_address_uv)                     as myCoinsTaskcomplete_address_uv,
    sum(myCoinsTaskupdate_photo_uv)                         as myCoinsTaskupdate_photo_uv,
    sum(myCoinsTaskreview_app_uv)                           as myCoinsTaskreview_app_uv,
    sum(myCoinsTaskopen_notification_uv)                    as myCoinsTaskopen_notification_uv,
    sum(myCoinsTaskcomplete_shopping_uv)                    as myCoinsTaskcomplete_shopping_uv,
    nvl(sum(mission_all_uv) / sum(check_in_uv), 0) * 100    as mission_rate
from (
         select activate_range,
                region_code,
                gmv_stage,
                check_in_uv as check_in_uv,
                0           as myCoinsTaskregiste_account_uv,
                0           as myCoinsTaskcomplete_address_uv,
                0           as myCoinsTaskupdate_photo_uv,
                0           as myCoinsTaskreview_app_uv,
                0           as myCoinsTaskopen_notification_uv,
                0           as myCoinsTaskcomplete_shopping_uv,
                0           as mission_all_uv
         from tmp.tmp_check_in_p_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                             as check_in_uv,
                myCoinsTaskregiste_account_uv as myCoinsTaskregiste_account_uv,
                0                             as myCoinsTaskcomplete_address_uv,
                0                             as myCoinsTaskupdate_photo_uv,
                0                             as myCoinsTaskreview_app_uv,
                0                             as myCoinsTaskopen_notification_uv,
                0                             as myCoinsTaskcomplete_shopping_uv,
                0                             as mission_all_uv
         from tmp_myCoinsTaskregiste_account_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                              as check_in_uv,
                0                              as myCoinsTaskregiste_account_uv,
                myCoinsTaskcomplete_address_uv as myCoinsTaskcomplete_address_uv,
                0                              as myCoinsTaskupdate_photo_uv,
                0                              as myCoinsTaskreview_app_uv,
                0                              as myCoinsTaskopen_notification_uv,
                0                              as myCoinsTaskcomplete_shopping_uv,
                0                              as mission_all_uv
         from tmp_myCoinsTaskcomplete_address_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                          as check_in_uv,
                0                          as myCoinsTaskregiste_account_uv,
                0                          as myCoinsTaskcomplete_address_uv,
                myCoinsTaskupdate_photo_uv as myCoinsTaskupdate_photo_uv,
                0                          as myCoinsTaskreview_app_uv,
                0                          as myCoinsTaskopen_notification_uv,
                0                          as myCoinsTaskcomplete_shopping_uv,
                0                          as mission_all_uv
         from tmp_myCoinsTaskupdate_photo_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                        as check_in_uv,
                0                        as myCoinsTaskregiste_account_uv,
                0                        as myCoinsTaskcomplete_address_uv,
                0                        as myCoinsTaskupdate_photo_uv,
                myCoinsTaskreview_app_uv as myCoinsTaskreview_app_uv,
                0                        as myCoinsTaskopen_notification_uv,
                0                        as myCoinsTaskcomplete_shopping_uv,
                0                        as mission_all_uv
         from tmp_myCoinsTaskreview_app_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                               as check_in_uv,
                0                               as myCoinsTaskregiste_account_uv,
                0                               as myCoinsTaskcomplete_address_uv,
                0                               as myCoinsTaskupdate_photo_uv,
                0                               as myCoinsTaskreview_app_uv,
                myCoinsTaskopen_notification_uv as myCoinsTaskopen_notification_uv,
                0                               as myCoinsTaskcomplete_shopping_uv,
                0                               as mission_all_uv
         from tmp.tmp_myCoinsTaskopen_notification_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                               as check_in_uv,
                0                               as myCoinsTaskregiste_account_uv,
                0                               as myCoinsTaskcomplete_address_uv,
                0                               as myCoinsTaskupdate_photo_uv,
                0                               as myCoinsTaskreview_app_uv,
                0                               as myCoinsTaskopen_notification_uv,
                myCoinsTaskcomplete_shopping_uv as myCoinsTaskcomplete_shopping_uv,
                0                               as mission_all_uv
         from tmp.tmp_myCoinsTaskcomplete_shopping_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0              as check_in_uv,
                0              as myCoinsTaskregiste_account_uv,
                0              as myCoinsTaskcomplete_address_uv,
                0              as myCoinsTaskupdate_photo_uv,
                0              as myCoinsTaskreview_app_uv,
                0              as myCoinsTaskopen_notification_uv,
                0              as myCoinsTaskcomplete_shopping_uv,
                mission_all_uv as mission_all_uv
         from tmp_mission_all_uv
     ) tmp
group by activate_range,
         region_code,
         gmv_stage
         ;
"

spark-sql \
--conf "spark.app.name=ads_vova_check_in_mission_huachen" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
