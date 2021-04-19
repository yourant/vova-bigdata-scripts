#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$pre_date"

sh /mnt/vova-bigdata-scripts/vova/es_export/export2ES.sh --sql="
select
*
from mlb.mlb_vova_rec_b_goods_score_all_d
where pt='${pre_date}'
" --index=mlb_vova_rec_b_goods_score_all_d

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

