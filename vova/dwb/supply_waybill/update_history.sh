#!/bin/bash
end_date=`date -d "-2 day" +%Y%m%d`
start_date=`date -d "-26 day" +%Y%m%d`
#start_date='20200525'
while [ ${start_date} -le ${end_date} ]; do
    cur_date=`date -d "${end_date}" +%Y-%m-%d`
    sh /mnt/vova-bigdata-scripts/vova/dwb/supply_waybill/update.sh ${cur_date}
    echo "${cur_date}"
    end_date=`date -d "${end_date} -1 day" +%Y%m%d`
done
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
