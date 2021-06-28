#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sh /mnt/vova-bigdata-scripts/common/table_check.sh  ads.ads_vova_bod_heat_rank 800 "pt='${pre_date}'"

if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists rec_recall.ads_vova_bod_heat_rank_pre;
drop table if exists rec_recall.ads_vova_bod_heat_rank_new;
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_bod_heat_rank_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`bod_id\` int(11) NOT NULL COMMENT '榜单id',
  \`rank\` int(11) NOT NULL COMMENT '榜单热度排名',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`),
  KEY \`bod_id\` (\`bod_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='榜单热度排名(知识图谱榜单&场景式榜单)';
CREATE TABLE IF NOT EXISTS \`rec_recall\`.\`ads_vova_bod_heat_rank\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`bod_id\` int(11) NOT NULL COMMENT '榜单id',
  \`rank\` int(11) NOT NULL COMMENT '榜单热度排名',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`),
  KEY \`bod_id\` (\`bod_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='榜单热度排名(知识图谱榜单&场景式榜单)';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ads_vova_bod_heat_rank_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_bod_heat_rank \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns bod_id,rank \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.ads_vova_bod_heat_rank to rec_recall.ads_vova_bod_heat_rank_pre,rec_recall.ads_vova_bod_heat_rank_new to rec_recall.ads_vova_bod_heat_rank;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi