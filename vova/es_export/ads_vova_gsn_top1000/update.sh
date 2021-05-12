#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
echo "$pre_date"
fi

sh /mnt/vova-bigdata-scripts/vova/es_export/export2ES.sh \
--sql="select * from ads.ads_vova_gsn_top1000 where pt='${pre_date}'" \
--index=ads_gsn_top1000 \
--id="${pre_date}" \
--mode=append \
--col_id=ctry,rec_page_code,gs_id
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
