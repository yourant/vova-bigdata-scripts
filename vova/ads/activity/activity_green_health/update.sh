#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
echo "cur_date: ${cur_date}"

sql="
insert overwrite table tmp.tmp_green_health_goods_id partition(pt='${cur_date}')
select distinct goods_id
from (
select goods_id
from dwb.dwb_vova_red_packet_goods
where pt = '${cur_date}'
and gsn_status = 3
union all
select goods_id
from tmp.ttt_ysj
) tmp
;


"