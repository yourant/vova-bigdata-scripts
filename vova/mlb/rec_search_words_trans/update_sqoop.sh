#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo ${pre_date}

query_translation=`aws s3 ls s3://vova-mlb/REC/data/base/mlb_vova_user_query_translation_d/pt=${pre_date}/ | wc -l`
if [ ${query_translation} -eq 0 ]; then
  echo "pt=${pre_date} query_translation num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_user_query_translation_d;"


if [ $? -ne 0 ];then
  exit 1
fi


mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
drop table if exists themis.query_translation_new;
drop table if exists themis.query_translation_pre;
CREATE TABLE themis.query_translation_new
(
    clk_from      varchar(2048) COMMENT 'result',
    translation_query  varchar(2048) COMMENT 'source',
    update_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    KEY idx_result (clk_from) USING BTREE,
    KEY idx_source (translation_query) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索词翻译表';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table query_translation_new \
--m 2 \
--update-key "clk_from,translation_query" \
--update-mode allowinsert \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_user_query_translation_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns clk_from,translation_query \
--fields-terminated-by '\t'



if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.query_translation to themis.query_translation_pre;
rename table themis.query_translation_new to themis.query_translation;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

