hive -e
"
insert overwrite table tmp.rpt_mct_rank_detail_country_cr
select
country,
round(avg(payed_user_num/dau),4) as rate
from
rpt.rpt_main_process
where datasource ='vova' and
country !='all' and
os_type='all' and
main_channel ='all' and
is_new='all'
group by country
having country !='';
"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table country_cr \
--update-key "country" \
--update-mode allowinsert \
--hcatalog-database tmp \
--hcatalog-table rpt_mct_rank_detail_country_cr \
--fields-terminated-by '\001'