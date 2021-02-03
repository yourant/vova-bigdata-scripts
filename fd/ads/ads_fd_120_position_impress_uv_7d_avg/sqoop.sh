#bin/sh
table="ads_fd_120_position_impression_uv_7d_avg"
user="gaohaitao"

base_path="/mnt/vova-bigdata-scripts/fd/dwb"

if [ ! -n "$1" ]; then
  pt=$(date -d "-1 days" +"%Y-%m-%d")
  pt_8d=$(date -d "-8 days" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
  pt_8d=`date -d "$1 -7 days" +%Y-%m-%d`
fi
echo "pt: ${pt}"
echo "pt_7d: ${pt_8d}"

ar_host="artemis-data.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com"
ar_user="data-report"
ar_pwd='C27PoowhAZIU$LHeI%Gs'

old_data="DELETE FROM data_report.rpt_120_position_impression_avg  WHERE data_time = '${pt_8d}';"
result_pt=`mysql -h "${ar_host}" -u"${ar_user}" -p"${ar_pwd}" -N -e "${old_data}"`

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://artemis-data.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com:3306/data_report \
--username data-report \
--password 'C27PoowhAZIU$LHeI%Gs' \
--table rpt_120_position_impression_avg \
--update-key "data_time,project_name,platform_name,route_sn,route_name,country,absolute_position" \
--columns project_name,platform_name,route_sn,route_name,country,absolute_position,impression_uv_avg,data_time \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_fd_120_position_impression_uv_7d_avg \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
-m 1

if [ $? -ne 0 ]; then
  exit 1
fi

echo "table [$table] is finished !"