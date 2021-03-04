#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date +%Y-%m-%d)
fi

echo ${pre_date}

file_num=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/graph_embedding/no_brand/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi

hive -e "msck repair table mlb.mlb_vova_graph_embedding_no_brand_data;"

if [ $? -ne 0 ];then
  exit 1
fi


mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
drop table if exists themis.graph_embedding_no_brand_data_new;
drop table if exists themis.graph_embedding_no_brand_data_pre;
CREATE TABLE themis.graph_embedding_no_brand_data_new
(
    id1      bigint COMMENT 'id1',
    id2      bigint COMMENT 'id2',
    score    DOUBLE COMMENT 'score',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    PRIMARY KEY (id1, id2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='keywords_clustering_result';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table graph_embedding_no_brand_data_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_graph_embedding_no_brand_data \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns id1,id2,score \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.graph_embedding_no_brand_data to themis.graph_embedding_no_brand_data_pre;
rename table themis.graph_embedding_no_brand_data_new to themis.graph_embedding_no_brand_data;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


