#!/bin/bash
pt=$1
if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y-%m-%d`
fi

db_username=""
db_password=""

echo "import to mysql database"

#if error
if [ $? -ne 0 ];then
   exit 1
fi

drop_table="
drop table if exists themis.ads_query_dic_tmp
;
drop table if exists themis.ads_query_dic_old
;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u ${db_username} -p${db_password} -e "${drop_table}"
if [ $? -ne 0 ];then
  exit 1
fi
echo "done drop table"
create_tmp_table="
CREATE TABLE themis.ads_query_dic_tmp  (
  id int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  \`key\` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  type tinyint(1) NOT NULL COMMENT '“0”-词，“1”-性别',
  cat_info text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  update_time timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  create_time timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (id) USING BTREE,
  UNIQUE INDEX uk_key(\`key\`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u ${db_username} -p${db_password} -e "${create_tmp_table}"
if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username ${db_username}  --password ${db_password} \
--table ads_query_dic_tmp \
--update-key "key" \
--update-mode allowinsert \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_word_prob_json \
--fields-terminated-by '\001' \
--columns "key,type,cat_info"

#if error
if [ $? -ne 0 ];then
   exit 1
fi

#sqoop?~H| ?~Y?mysql?~U??~M?
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u ${db_username} -p${db_password} <<EOF
rename table themis.ads_query_dic to themis.ads_query_dic_old;
rename table themis.ads_query_dic_tmp to themis.ads_query_dic;
EOF

#if error
if [ $? -ne 0 ];then
   exit 1
fi