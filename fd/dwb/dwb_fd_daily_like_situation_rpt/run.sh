#!/bin/sh
if [ ! -n "$1" ] ;then
    #获取前一小时所属的日期
    pt=`date -d "- 1 days" +"%Y-%m-%d"`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1

fi

#hive sql中使用的变量
echo '当前脚本执行时间:' $pt

sql="
insert overwrite table dwb.dwb_fd_daily_like_situation_rpt partition(pt='$pt')
select /*+ REPARTITION(1) */ batch,
       virtual_goods_id,
       project,
       nvl(country, 'ALL'),
       nvl(platform_type, 'ALL'),
       count(if(event = 'goods_like', session_id, null))       as like_num,
       count(if(event = 'goods_dislike', session_id, null))    as unlike_num,
       count(if(event = 'goods_impression', session_id, null)) as impressions
from (
         select nvl(batch, 'NALL')            as batch,
                nvl(virtual_goods_id, 'NALL') as virtual_goods_id,
                nvl(project, 'NALL')          as project,
                nvl(country, 'NALL')          as country,
                nvl(platform_type, 'NALL')    as platform_type,
                event,
                session_id
         from (
                  select get_json_object(goods_event_struct.extra, '$.element_batch') as batch,
                         goods_event_struct.virtual_goods_id                          as virtual_goods_id,
                         project,
                         country,
                         platform_type,
                         'goods_impression'                                            as event,
                         session_id
                  from ods_fd_snowplow.ods_fd_snowplow_goods_event
                  where pt = '$pt'
                    and length(country)<=2
                    and event_name = 'goods_impression'
                    and goods_event_struct.list_type = '/InspiredList'
                  union all
                  select get_json_object(element_event_struct.extra, '$.element_batch') as batch,
                         element_event_struct.element_id                                as virtual_goods_id,
                         project,
                         country,
                         platform_type,
                         case lower(element_event_struct.element_name)
                             when lower('InspiredGoodsLike') then 'goods_like'
                             when lower('InspiredGoodsDisLike') then 'goods_dislike'
                             else 'null'
                             end                                                        as event,

                         session_id
                  from ods_fd_snowplow.ods_fd_snowplow_element_event
                  where pt = '$pt'
                    and length(country)<=2
                    and event_name in ('common_click')
                    and lower(element_event_struct.element_name) in
                        (lower('InspiredGoodsLike'), lower('InspiredGoodsDisLike'))
                        ) t1
         )t2
group by batch, virtual_goods_id, project, country, platform_type
    grouping sets (
    ( batch, virtual_goods_id, project, country, platform_type),
    ( batch, virtual_goods_id, project, country),
    ( batch, virtual_goods_id, project, platform_type ),
    ( batch, virtual_goods_id, project)
    );
"

echo '当前执行的sql:' $sql

spark-sql --conf "spark.app.name=dwb_fd_daily_like_situation_gaohaitao"  --conf "spark.dynamicAllocation.maxExecutors=60" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "dwb_fd_daily_like_situation table is finished !"
