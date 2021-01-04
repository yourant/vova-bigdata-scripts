#bin/sh
table="dwb_fd_income_cost"
user="gaohaitao"

base_path="/mnt/vova-bigdata-scripts/fd/dwb"

if [ ! -n "$1" ]; then
  pt=$(date -d "- 1 hours" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
fi
echo "pt: ${pt}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://artemis-data.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com:3306/data_report \
--username data-report \
--password 'C27PoowhAZIU$LHeI%Gs' \
--table rpt_income_cost \
--update-key "pt, party_name, country_code, country_name, dimension_type" \
--columns batch,goods_id,virtual_goods_id,project_name,country_code,platform,like_num,unlike_num,impressions,pt \
--update-mode allowinsert \
--hcatalog-database dwb \
--hcatalog-table dwb_fd_income_cost \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by '\001'

if [ $? -ne 0 ]; then
  exit 1
fi

echo "table [$table] is finished !"