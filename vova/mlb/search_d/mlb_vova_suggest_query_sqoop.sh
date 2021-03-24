#!/bin/bash
pt=$1
if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y-%m-%d`
fi

db_username=""
db_password=""

echo "import to mysql database"

drop_table="
drop table if exists themis.ads_suggest_query_tmp
;
drop table if exists themis.ads_suggest_query_old
;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u ${db_username} -p${db_password} -e "${drop_table}"
if [ $? -ne 0 ];then
  exit 1
fi

echo "done drop table"
create_tmp_table="
CREATE TABLE themis.ads_suggest_query_tmp (
  id int(11) NOT NULL AUTO_INCREMENT,
  query varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  weight int(11) NOT NULL,
  create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY query (query) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=262294 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u ${db_username} -p${db_password} -e "${create_tmp_table}"
if [ $? -ne 0 ];then
  exit 1
fi
#sqoop?~H| ?~Y?mysql?~U??~M?
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username ${db_username}  --password ${db_password} \
--table ads_suggest_query_tmp \
--update-key "query" \
--update-mode allowinsert \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_suggest_query_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
--columns "query, weight"

#if error
if [ $? -ne 0 ];then
   exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u ${db_username} -p${db_password} <<EOF
rename table themis.ads_suggest_query to themis.ads_suggest_query_old;
rename table themis.ads_suggest_query_tmp to themis.ads_suggest_query;
EOF

#if error
if [ $? -ne 0 ];then
   exit 1
fi