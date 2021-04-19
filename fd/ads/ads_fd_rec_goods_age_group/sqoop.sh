#bin/sh
table="ads_fd_goods_inspired"
user="lujiaheng"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date -d "$1" +"%Y-%m-%d %H")
if [ "$#" -ne 2 ]; then
  pt=$(date -d "$ts" +"%Y-%m-%d")
else
  pt=$2
fi
echo "pt: ${pt}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://bd-warehouse-maxscale.gitvv.com:3313/data_report \
-m 1 \
--username data-report \
--password 'C27PoowhAZIU$LHeI%Gs' \
--table goods_age_group \
--update-key "goods_id, age_group" \
--columns goods_id,age_group \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_fd_goods_age_group \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by '\001'

if [ $? -ne 0 ]; then
  exit 1
fi

echo "table [$table] is finished !"