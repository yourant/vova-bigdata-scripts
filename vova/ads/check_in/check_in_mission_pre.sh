#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
--完成注册任务的device+buyer
insert overwrite table tmp.tmp_myCoinsTaskregiste_accountt
select device_id,buyer_id
from (
select device_id,buyer_id,
row_number() over(partition by device_id order by buyer_id desc) rk
from dwd.dwd_vova_log_common_click
where datasource = 'vova'
and pt = '${cur_date}'
and page_code = 'coins_rewards'
and element_name = 'myCoinsTaskregiste_account'
) tmp
where rk = 1
;
--完成填写地址的device+buyer
insert overwrite table tmp.tmp_myCoinsTaskcomplete_address
select device_id,buyer_id
from (
select device_id,buyer_id,
row_number() over(partition by device_id order by buyer_id desc) rk
from dwd.dwd_vova_log_common_click
where datasource = 'vova'
and pt = '${cur_date}'
and page_code = 'coins_rewards'
and element_name = 'myCoinsTaskcomplete_address'
) tmp
where rk = 1
;
--完成修改头像的device+buyer
insert overwrite table tmp.tmp_myCoinsTaskupdate_photo
select device_id,buyer_id
from (
select device_id,buyer_id,
row_number() over(partition by device_id order by buyer_id desc) rk
from dwd.dwd_vova_log_common_click
where datasource = 'vova'
and pt = '${cur_date}'
and page_code = 'coins_rewards'
and element_name = 'myCoinsTaskupdate_photo'
) tmp
where rk = 1
;
--完成评价APP的device+buyer
insert overwrite table tmp.tmp_myCoinsTaskreview_app
select device_id,buyer_id
from (
select device_id,buyer_id,
row_number() over(partition by device_id order by buyer_id desc) rk
from dwd.dwd_vova_log_common_click
where datasource = 'vova'
and pt = '${cur_date}'
and page_code = 'coins_rewards'
and element_name = 'myCoinsTaskreview_app'
) tmp
where rk = 1
;
--完成开启通知任务的device+buyer
insert overwrite table tmp.tmp_myCoinsTaskopen_notification
select device_id,buyer_id
from (
select device_id,buyer_id,
row_number() over(partition by device_id order by buyer_id desc) rk
from dwd.dwd_vova_log_common_click
where datasource = 'vova'
and pt = '${cur_date}'
and page_code = 'coins_rewards'
and element_name = 'myCoinsTaskopen_notification'
) tmp
where rk = 1
;
--完成购买任务的device+buyer
insert overwrite table tmp.tmp_myCoinsTaskcomplete_shopping
select device_id,buyer_id
from (
select device_id,buyer_id,
row_number() over(partition by device_id order by buyer_id desc) rk
from dwd.dwd_vova_log_common_click
where datasource = 'vova'
and pt = '${cur_date}'
and page_code = 'coins_rewards'
and element_name = 'myCoinsTaskcomplete_shopping'
) tmp
where rk = 1
;
--签到UV
insert overwrite table tmp.tmp_check_in_p_uv
select activate_range,
       region_code,
       gmv_stage,
       count(distinct d)                                    as check_in_uv
from (
         select a.device_id               as d,
                case
                    when a.device_id is null then 5
                    when to_date(dd.activate_time) = '${cur_date}' then 1
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
                    when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                         datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
                    else 4
                    end                   as activate_range,
                nvl(dd.region_code, 'NA') as region_code,
                nvl(y.gmv_stage,5) as gmv_stage,
                row_number()                 over(partition by a.device_id order by a.buyer_id desc) rk
         from dwd.dwd_vova_log_screen_view a
                  left join dim.dim_vova_devices dd
                            on a.device_id = dd.device_id
                                and a.datasource = dd.datasource
                  left join (select *
                    from ads.ads_vova_buyer_portrait_feature
                    where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                            on y.buyer_id = a.buyer_id
                                and a.datasource = y.datasource
         where a.datasource = 'vova'
           and a.pt = '${cur_date}'
           and a.page_code = 'coins_rewards'
     ) t2
where rk = 1
group by activate_range,
         region_code,
         gmv_stage with cube
;
--完成购买任务的uv
insert overwrite table tmp.tmp_myCoinsTaskcomplete_shopping_uv
select
activate_range,
region_code,
gmv_stage,
count(distinct device_id) as myCoinsTaskcomplete_shopping_uv
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
from tmp.tmp_myCoinsTaskcomplete_shopping t
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
;
--完成开启通知的uv
insert overwrite table tmp.tmp_myCoinsTaskopen_notification_uv
select
activate_range,
region_code,
gmv_stage,
count(distinct device_id) as myCoinsTaskopen_notification_uv
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
from tmp.tmp_myCoinsTaskopen_notification t
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
;
"

spark-sql \
--conf "spark.app.name=ads_vova_check_in_mission_pre_huachen" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi