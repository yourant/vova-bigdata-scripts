#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_buyer_portrait_brand_likes_pre;
drop table if exists themis.ads_buyer_portrait_brand_likes_new;
create table themis.ads_buyer_portrait_brand_likes_new(
\`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`buyer_id\`                    bigint COMMENT 'd_买家id',
\`brand_id\`                    bigint COMMENT 'd_品牌id',
\`expre_cnt_1w\`                bigint COMMENT 'i_近7天品牌曝光次数',
\`expre_cnt_15d\`               bigint COMMENT 'i_近15天品牌曝光次数',
\`expre_cnt_1m\`                bigint COMMENT 'i_近30天品牌曝光次数',
\`clk_cnt_1w\`                  bigint COMMENT 'i_近7天品牌点击次数',
\`clk_cnt_15d\`                 bigint COMMENT 'i_近15天品牌点击次数',
\`clk_cnt_1m\`                  bigint COMMENT 'i_近30天品牌点击次数',
\`clk_valid_cnt_1w\`            bigint COMMENT 'i_近7天品牌有效点击次数',
\`clk_valid_cnt_15d\`           bigint COMMENT 'i_近15天品牌有效点击次数',
\`clk_valid_cnt_1m\`            bigint COMMENT 'i_近30天品牌有效点击次数',
\`collect_cnt_1w\`              bigint COMMENT 'i_近7天品牌收藏次数',
\`collect_cnt_15d\`             bigint COMMENT 'i_近15天品牌收藏次数',
\`collect_cnt_1m\`              bigint COMMENT 'i_近30天品牌收藏次数',
\`add_cat_cnt_1w\`              bigint COMMENT 'i_近7天品牌加购次数',
\`add_cat_cnt_15d\`             bigint COMMENT 'i_近15天品牌加购次数',
\`add_cat_cnt_1m\`              bigint COMMENT 'i_近30天品牌加购次数',
\`ord_cnt_1w\`                  bigint COMMENT 'i_近7天品牌购买次数',
\`ord_cnt_15d\`                 bigint COMMENT 'i_近15天品牌购买次数',
\`ord_cnt_1m\`                  bigint COMMENT 'i_近30天品牌购买次数',
\`last_update_time\`            timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (\`id\`) USING BTREE,
UNIQUE KEY \`buyer_id_brand_id\` (\`buyer_id\`,\`brand_id\`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户画像品牌偏好统计表(有近15日点击数据)';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=3000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis?rewriteBatchedStatements=true \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_buyer_portrait_brand_likes_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_portrait_brand_likes_with_click_15d \
--columns buyer_id,brand_id,expre_cnt_1w,expre_cnt_15d,expre_cnt_1m,clk_cnt_1w,clk_cnt_15d,clk_cnt_1m,clk_valid_cnt_1w,clk_valid_cnt_15d,clk_valid_cnt_1m,collect_cnt_1w,collect_cnt_15d,collect_cnt_1m,add_cat_cnt_1w,add_cat_cnt_15d,add_cat_cnt_1m,ord_cnt_1w,ord_cnt_15d,ord_cnt_1m \
--fields-terminated-by '\001' \
--batch

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_buyer_portrait_brand_likes to themis.ads_buyer_portrait_brand_likes_pre,themis.ads_buyer_portrait_brand_likes_new to themis.ads_buyer_portrait_brand_likes;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
