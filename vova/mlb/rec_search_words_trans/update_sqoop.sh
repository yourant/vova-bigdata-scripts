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


hive -e "msck repair table mlb.mlb_vova_user_query_translation_d_new;"


if [ $? -ne 0 ];then
  exit 1
fi


mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.query_translation_new;
drop table if exists themis.query_translation_pre;
CREATE TABLE themis.query_translation_new
(
    clk_from      varchar(2048) COMMENT 'source',
    translation_query  varchar(2048) COMMENT 'result',
    update_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    KEY idx_source (clk_from) USING BTREE,
    KEY idx_result (translation_query) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索词翻译表';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table query_translation_new \
--m 10 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_user_query_translation_d_new \
--columns clk_from,translation_query \
--fields-terminated-by '\t'




if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.query_translation to themis.query_translation_pre;
rename table themis.query_translation_new to themis.query_translation;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

