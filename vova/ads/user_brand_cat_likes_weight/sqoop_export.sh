#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.user_brand_cat_likes_weight_pre;
drop table if exists themis.user_brand_cat_likes_weight_now;
CREATE TABLE if not exists \`themis\`.\`user_brand_cat_likes_weight_now\`  (
  \`id\` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  \`user_id\` int(11) NOT NULL,
  \`brand\` varchar(108) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci  DEFAULT '' COMMENT '品牌偏好top20(int)',
  \`first_cat\` varchar(56) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '一级品类top10(int)',
  \`second_cat\` varchar(56) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '二级品类top10(int)',
  \`update_time\` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  \`create_time\` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE INDEX \`uk_user_id\`(\`user_id\`) USING BTREE COMMENT '一个用户最多一行偏好记录'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
CREATE TABLE  if not exists \`themis\`.\`user_brand_cat_likes_weight\`  (
  \`id\` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  \`user_id\` int(11) NOT NULL,
  \`brand\` varchar(108) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci  DEFAULT '' COMMENT '品牌偏好top20(int)',
  \`first_cat\` varchar(56) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '一级品类top10(int)',
  \`second_cat\` varchar(56) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '二级品类top10(int)',
  \`update_time\` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  \`create_time\` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE INDEX \`uk_user_id\`(\`user_id\`) USING BTREE COMMENT '一个用户最多一行偏好记录'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8192 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table user_brand_cat_likes_weight_now \
--hcatalog-database ads \
--hcatalog-table ads_vova_user_brand_cat_likes_weight \
--columns user_id,brand,first_cat,second_cat \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.user_brand_cat_likes_weight to themis.user_brand_cat_likes_weight_pre,themis.user_brand_cat_likes_weight_now to themis.user_brand_cat_likes_weight;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
