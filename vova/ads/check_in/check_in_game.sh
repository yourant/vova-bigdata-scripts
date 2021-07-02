#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
--签到页UV+积分游戏UV+抽奖游戏页UV+运动奖励页UV
insert overwrite table tmp.tmp_check_in_game_uv
select activate_range,
       region_code,
       gmv_stage,
       sum(if(page_code = 'coins_rewards', 1, 0))        as check_in_uv,
       sum(if(page_code = 'coins_game', 1, 0))           as check_in_game_uv,
       sum(if(page_code = 'coins_draw', 1, 0))           as check_in_draw_uv,
       sum(if(page_code = 'daily_sports_welfare', 1, 0)) as check_in_sport_uv
from (
         select a.device_id               as d,
                a.page_code,
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
                nvl(y.gmv_stage, 5)       as gmv_stage,
                row_number()                 over(partition by a.page_code,a.device_id order by a.buyer_id desc) rk
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
     ) t2
where rk = 1
group by activate_range,
         region_code,
         gmv_stage with cube
;

-- click uv
insert overwrite table tmp.tmp_check_in_click_uv
select activate_range,
       region_code,
       gmv_stage,
       sum(if(page_code = 'coins_game' and element_name = 'CoinsGameButtonNotInvolved', 1,
              0))                                                                               as CoinsGameButtonNotInvolved_uv,
       sum(if(page_code = 'coins_draw' and element_name = 'freePlayButton', 1, 0))              as freePlayButton_uv,
       sum(if(page_code = 'coins_draw' and element_name = '20PlayButton', 1, 0))                as 20PlayButton_uv,
       sum(if(page_code = 'daily_sports_welfare' and element_name = 'sportsWelOpen', 1, 0))     as sportsWelOpen_uv,
       sum(if(page_code = 'daily_sports_welfare' and element_name = 'sportsWelExchange', 1, 0)) as sportsWelExchange_uv
from (
         select a.device_id               as d,
                a.page_code,
                a.element_name,
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
                nvl(y.gmv_stage, 5)       as gmv_stage,
                row_number()                 over(partition by a.page_code,a.element_name,a.device_id order by a.buyer_id desc) rk
         from dwd.dwd_vova_log_common_click a
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
     ) t2
where rk = 1
group by activate_range,
         region_code,
         gmv_stage with cube
         ;
-- click pv
insert overwrite table tmp.tmp_check_in_click_pv
select case
           when a.device_id is null then 5
           when to_date(dd.activate_time) = '${cur_date}' then 1
           when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
           when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
           else 4
           end                   as activate_range,
       nvl(dd.region_code, 'NA') as region_code,
       nvl(y.gmv_stage, 5)       as gmv_stage,
       count(*)                  as 20PlayButton_pv
from dwd.dwd_vova_log_common_click a
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
  and a.page_code = 'coins_draw'
  and a.element_name = '20PlayButton'
group by case
             when a.device_id is null then 5
             when to_date(dd.activate_time) = '${cur_date}' then 1
             when datediff('${cur_date}', to_date(dd.activate_time)) >= 1 and
                  datediff('${cur_date}', to_date(dd.activate_time)) < 7 then 2
             when datediff('${cur_date}', to_date(dd.activate_time)) >= 7 and
                  datediff('${cur_date}', to_date(dd.activate_time)) < 30 then 3
             else 4
             end,
         nvl(dd.region_code, 'NA'),
         nvl(y.gmv_stage, 5) with cube
;
insert overwrite table ads.ads_vova_check_in_game PARTITION (pt='${cur_date}')
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
    sum(check_in_uv) as check_in_uv,
    sum(check_in_game_uv) as check_in_game_uv,
    sum(CoinsGameButtonNotInvolved_uv) as CoinsGameButtonNotInvolved_uv,
    sum(check_in_draw_uv) as check_in_draw_uv,
    sum(freePlayButton_uv) as freePlayButton_uv,
    sum(20PlayButton_uv) as 20PlayButton_uv,
    sum(20PlayButton_pv) as 20PlayButton_pv,
    sum(check_in_sport_uv) as check_in_sport_uv,
    sum(sportsWelOpen_uv) as sportsWelOpen_uv,
    sum(sportsWelExchange_uv) as sportsWelExchange_uv
from (
         select activate_range,
                region_code,
                gmv_stage,
                check_in_uv       as check_in_uv,
                check_in_game_uv  as check_in_game_uv,
                0                 as CoinsGameButtonNotInvolved_uv,
                check_in_draw_uv  as check_in_draw_uv,
                0                 as freePlayButton_uv,
                0                 as 20PlayButton_uv,
                0                 as 20PlayButton_pv,
                check_in_sport_uv as check_in_sport_uv,
                0                 as sportsWelOpen_uv,
                0                 as sportsWelExchange_uv
         from tmp.tmp_check_in_game_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0                             as check_in_uv,
                0                             as check_in_game_uv,
                CoinsGameButtonNotInvolved_uv as CoinsGameButtonNotInvolved_uv,
                0                             as check_in_draw_uv,
                freePlayButton_uv             as freePlayButton_uv,
                20PlayButton_uv               as 20PlayButton_uv,
                0                             as 20PlayButton_pv,
                0                             as check_in_sport_uv,
                sportsWelOpen_uv              as sportsWelOpen_uv,
                sportsWelExchange_uv          as sportsWelExchange_uv
         from tmp.tmp_check_in_click_uv
         union all
         select activate_range,
                region_code,
                gmv_stage,
                0               as check_in_uv,
                0               as check_in_game_uv,
                0               as CoinsGameButtonNotInvolved_uv,
                0               as check_in_draw_uv,
                0               as freePlayButton_uv,
                0               as 20PlayButton_uv,
                20PlayButton_pv as 20PlayButton_pv,
                0               as check_in_sport_uv,
                0               as sportsWelOpen_uv,
                0               as sportsWelExchange_uv
         from tmp.tmp_check_in_click_pv
     ) tmp
group by
    activate_range,
    region_code,
    gmv_stage
;

"

spark-sql \
--conf "spark.app.name=ads_vova_check_in_game_huachen" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi