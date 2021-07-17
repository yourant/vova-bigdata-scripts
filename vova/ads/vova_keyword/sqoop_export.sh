#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_result_page_keyword_rank_data 800 "pt='${pre_date}'"

sql="
drop table if exists rec_recall.ads_vova_result_page_keyword_rank_data_pre;
drop table if exists rec_recall.ads_vova_result_page_keyword_rank_data_new;
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_result_page_keyword_rank_data_new\` (
\`id\` int(11) NOT NULL AUTO_INCREMENT,
\`type\` int(4) NOT NULL COMMENT '计算类型(1 goods_id,2 cat_id,3 query)',
\`type_value\` varchar(255) NOT NULL COMMENT '类型值',
\`keyword_id\` int(4) NOT NULL COMMENT '关键词id',
\`rank\` int(4) NOT NULL COMMENT '排名',
\`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
KEY \`type_value\` (\`type_value\`),
KEY \`keyword_id\` (\`keyword_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='列表页&搜索结果页关键词排名数据';
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_result_page_keyword_rank_data\` (
\`id\` int(11) NOT NULL AUTO_INCREMENT,
\`type\` int(4) NOT NULL COMMENT '计算类型(1 goods_id,2 cat_id,3 query)',
\`type_value\` varchar(255) NOT NULL COMMENT '类型值',
\`keyword_id\` int(4) NOT NULL COMMENT '关键词id',
\`rank\` int(4) NOT NULL COMMENT '排名',
\`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
KEY \`type_value\` (\`type_value\`),
KEY \`keyword_id\` (\`keyword_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='列表页&搜索结果页关键词排名数据';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_result_page_keyword_rank_data_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_result_page_keyword_rank_data \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns type,type_value,keyword_id,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.ads_vova_result_page_keyword_rank_data to rec_recall.ads_vova_result_page_keyword_rank_data_pre,rec_recall.ads_vova_result_page_keyword_rank_data_new to rec_recall.ads_vova_result_page_keyword_rank_data;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi