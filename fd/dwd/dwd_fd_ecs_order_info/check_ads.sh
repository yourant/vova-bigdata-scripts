#!/bin/sh

if [ ! -n "$1" ]; then
  pt=$(date -d "- 1 days" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
fi
echo "pt: ${pt}"

ar_host="bd-warehouse-maxscale.gitvv.com"
ar_user="market"
ar_pwd="MyF4k2y9jJSv"

max_pt="select  max(date) from artemis.ads_adgroup_daily_flat_report;"
result_pt=`mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -P3312 -N -e "${max_pt}"`

if [ "${pt}" == "${result_pt}" ]; then
        echo 'artemis-db '${pt}'广告花费已经入库!'
        sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis.sh --db_code=ar --etl_type=ALL --table_name=ads_adgroup_daily_flat_report --partition_num=5 --period_type=day

else
        echo 'artemis-db '${pt}'广告花费未入库，等待...!'
        exit 1
fi