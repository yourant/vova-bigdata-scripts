#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sh /mnt/vova-bigdata-scripts/common/table_check.sh mlb.mlb_vova_kg_d 800 "pt='${pre_date}'"
if [ $? -ne 0 ];then
exit 1
fi
m_sql="
drop table if exists rec_recall.mlb_vova_kg_d_pre;
drop table if exists rec_recall.mlb_vova_kg_d_new;
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`mlb_vova_kg_d_new\` (
\`id\` int(11) NOT NULL AUTO_INCREMENT,
\`buyer_id\` int(11) NOT NULL COMMENT '用户id',
\`bod_id_list\` mediumtext NOT NULL COMMENT '榜单id列表',
\`score_list\` mediumtext NOT NULL COMMENT '分数列表',
\`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
KEY \`buyer_id\` (\`buyer_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-用户榜单偏好';
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`mlb_vova_kg_d\` (
\`id\` int(11) NOT NULL AUTO_INCREMENT,
\`buyer_id\` int(11) NOT NULL COMMENT '用户id',
\`bod_id_list\` mediumtext NOT NULL COMMENT '榜单id列表',
\`score_list\` mediumtext NOT NULL COMMENT '分数列表',
\`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
KEY \`buyer_id\` (\`buyer_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-用户榜单偏好';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${m_sql}"

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
--table mlb_vova_kg_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_kg_d \
--columns buyer_id,bod_id_list,score_list \
--hive-partition-key pt \
--hive-partition-value ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
exit 1
fi
echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
RENAME table rec_recall.mlb_vova_kg_d to rec_recall.mlb_vova_kg_d_pre,rec_recall.mlb_vova_kg_d_new to rec_recall.mlb_vova_kg_d;
EOF
echo "-------rename结束--------"
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/table_check.sh mlb.mlb_vova_kg_no_user_d 100 "pt='${pre_date}'"
if [ $? -ne 0 ];then
exit 1
fi
sql="
drop table if exists rec_recall.mlb_vova_kg_no_user_d_pre;
drop table if exists rec_recall.mlb_vova_kg_no_user_d_new;
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`mlb_vova_kg_no_user_d_new\` (
\`id\` int(11) NOT NULL AUTO_INCREMENT,
\`region_id\` int(11) NOT NULL COMMENT '用户id',
\`gender\` varchar(50) NOT NULL COMMENT '性别',
\`user_age_group\` varchar(50) NOT NULL COMMENT '年龄组',
\`bod_id_list\` mediumtext NOT NULL COMMENT '榜单id列表',
\`score_list\` mediumtext NOT NULL COMMENT '分数列表',
\`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
KEY \`union_index\` (\`region_id\`,\`gender\`,\`user_age_group\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-偏好召回';
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`mlb_vova_kg_no_user_d\` (
\`id\` int(11) NOT NULL AUTO_INCREMENT,
\`region_id\` int(11) NOT NULL COMMENT '用户id',
\`gender\` varchar(50) NOT NULL COMMENT '性别',
\`user_age_group\` varchar(50) NOT NULL COMMENT '年龄组',
\`bod_id_list\` mediumtext NOT NULL COMMENT '榜单id列表',
\`score_list\` mediumtext NOT NULL COMMENT '分数列表',
\`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
KEY \`union_index\` (\`region_id\`,\`gender\`,\`user_age_group\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-偏好召回';
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
--table mlb_vova_kg_no_user_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_kg_no_user_d \
--columns region_id,gender,user_age_group,bod_id_list,score_list \
--hive-partition-key pt \
--hive-partition-value ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
exit 1
fi
echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
RENAME table rec_recall.mlb_vova_kg_no_user_d to rec_recall.mlb_vova_kg_no_user_d_pre,rec_recall.mlb_vova_kg_no_user_d_new to rec_recall.mlb_vova_kg_no_user_d;
EOF
echo "-------rename结束--------"
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_knowledge_graph_bod_goods_rank_data 800 "pt='${pre_date}'"

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists rec_recall.ads_vova_knowledge_graph_bod_goods_rank_data_pre;
drop table if exists rec_recall.ads_vova_knowledge_graph_bod_goods_rank_data_new;
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_knowledge_graph_bod_goods_rank_data_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`bod_id\` int(11) NOT NULL COMMENT '榜单id',
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`rank\` int(11) NOT NULL COMMENT '商品评分排名',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`),
  KEY \`bod_id\` (\`bod_id\`),
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱榜单商品评分排名统计';
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_knowledge_graph_bod_goods_rank_data\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`bod_id\` int(11) NOT NULL COMMENT '榜单id',
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`rank\` int(11) NOT NULL COMMENT '商品评分排名',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`),
  KEY \`bod_id\` (\`bod_id\`),
  KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱榜单商品评分排名统计';
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
--table ads_vova_knowledge_graph_bod_goods_rank_data_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_knowledge_graph_bod_goods_rank_data \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns bod_id,goods_id,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.ads_vova_knowledge_graph_bod_goods_rank_data to rec_recall.ads_vova_knowledge_graph_bod_goods_rank_data_pre,rec_recall.ads_vova_knowledge_graph_bod_goods_rank_data_new to rec_recall.ads_vova_knowledge_graph_bod_goods_rank_data;

EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi