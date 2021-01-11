month=`date -d "${cur_date}" +%m`
echo "${month}"
sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis.sh --db_code=vtp --table_name=app_push_logs_${month} --inc_column=id --etl_type=INCID --period_type=day --partition_num=1
