#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_vova_buyer_portrait_second_category_likes_exp_pre;
drop table if exists themis.ads_vova_buyer_portrait_second_category_likes_exp_new;
create table if not exists themis.ads_vova_buyer_portrait_second_category_likes_exp_new(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`buyer_id\`                    bigint COMMENT 'd_买家id',
\`second_cat_id\`                      bigint COMMENT 'd_品类id',
\`likes_weight_short\`          decimal(13,2) COMMENT  'd_短期偏好',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
UNIQUE KEY \`buyer_id_second_cat_id\` (\`buyer_id\`,\`second_cat_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户画像子品类短期偏好';

create table if not exists themis.ads_vova_buyer_portrait_second_category_likes_exp(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`buyer_id\`                    bigint COMMENT 'd_买家id',
\`second_cat_id\`                      bigint COMMENT 'd_品类id',
\`likes_weight_short\`          decimal(13,2) COMMENT 'd_短期偏好',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (\`id\`) USING BTREE,
UNIQUE KEY \`buyer_id_second_cat_id\` (\`buyer_id\`,\`second_cat_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户画像子品类短期偏好';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=3000 \
-Dmapreduce.map.memory.mb=8096 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_vova_buyer_portrait_second_category_likes_exp_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_portrait_second_category_likes_exp \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns buyer_id,second_cat_id,likes_weight_short \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_vova_buyer_portrait_second_category_likes_exp to themis.ads_vova_buyer_portrait_second_category_likes_exp_pre,themis.ads_vova_buyer_portrait_second_category_likes_exp_new to themis.ads_vova_buyer_portrait_second_category_likes_exp;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi