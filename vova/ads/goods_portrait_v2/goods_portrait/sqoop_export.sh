#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_goods_portrait_pre;
drop table if exists themis.ads_goods_portrait_now;
CREATE TABLE if not exists \`themis\`.\`ads_goods_portrait_now\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`gs_id\` int NOT NULL DEFAULT '0' COMMENT 'd_商品id',
  \`cat_id\` int DEFAULT NULL COMMENT 'i_品类id',
  \`first_cat_id\` int DEFAULT NULL COMMENT 'i_一级品类id',
  \`second_cat_id\` int DEFAULT NULL COMMENT 'i_二级品类id',
  \`brand_id\` int DEFAULT NULL COMMENT 'i_品牌id',
  \`shop_price\` decimal(13,2) DEFAULT NULL COMMENT 'i_商品价格',
  \`gs_discount\` decimal(13,2) DEFAULT NULL COMMENT 'i_商品折扣',
  \`shipping_fee\` decimal(13,2) DEFAULT NULL COMMENT 'i_商品运费',
  \`mct_id\` int DEFAULT NULL COMMENT 'i_商家ID',
  \`comment_cnt_6m\` int DEFAULT NULL COMMENT 'i_近180天评论数',
  \`comment_good_cnt_6m\` int DEFAULT NULL COMMENT 'i_近180天好评数',
  \`comment_bad_cnt_6m\` int DEFAULT NULL COMMENT 'i_近180天差评数',
  \`gmv_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天gmv',
  \`gmv_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天gmv',
  \`gmv_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天gmv',
  \`sales_vol_1w\` int DEFAULT NULL COMMENT 'i_近7天销量',
  \`sales_vol_15d\` int DEFAULT NULL COMMENT 'i_近15天销量',
  \`sales_vol_1m\` int DEFAULT NULL COMMENT 'i_近30天销量',
  \`expre_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天曝光数',
  \`expre_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天曝光数',
  \`expre_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天曝光数',
  \`clk_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天点击数',
  \`clk_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天点击数',
  \`clk_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天点击数',
  \`collect_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天收藏数',
  \`collect_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天收藏数',
  \`collect_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天收藏数',
  \`add_cat_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天加购数',
  \`add_cat_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天加购数',
  \`add_cat_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天加购数',
  \`clk_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天点击率',
  \`clk_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天点击率',
  \`clk_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天点击率',
  \`pay_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天支付转换率',
  \`pay_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天支付转换率',
  \`pay_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天支付转换率',
  \`add_cat_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天加购转化率',
  \`add_cat_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天加购转化率',
  \`add_cat_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天加购转化率',
  \`cr_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天转换率',
  \`cr_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天转换率',
  \`cr_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天转换率',
  \`goods_id\` int(11) DEFAULT NULL COMMENT 'i_商品ID',
  \`goods_name\` varchar(512) DEFAULT NULL COMMENT 'i_商品名称',
  \`goods_sn\` varchar(50) DEFAULT NULL COMMENT 'i_商品所属sn',
  \`is_on_sale\` int(2) DEFAULT NULL COMMENT 'i_真实是否在售',
  \`is_recommend\` int(2) DEFAULT NULL COMMENT 'i_是否可推荐',
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`idx_gs_id\` (\`gs_id\`),
  KEY \`idx_second_cat_id\` (\`second_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品画像表';

CREATE TABLE if not exists \`themis\`.\`ads_goods_portrait\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`gs_id\` int NOT NULL DEFAULT '0' COMMENT 'd_商品id',
  \`cat_id\` int DEFAULT NULL COMMENT 'i_品类id',
  \`first_cat_id\` int DEFAULT NULL COMMENT 'i_一级品类id',
  \`second_cat_id\` int DEFAULT NULL COMMENT 'i_二级品类id',
  \`brand_id\` int DEFAULT NULL COMMENT 'i_品牌id',
  \`shop_price\` decimal(13,2) DEFAULT NULL COMMENT 'i_商品价格',
  \`gs_discount\` decimal(13,2) DEFAULT NULL COMMENT 'i_商品折扣',
  \`shipping_fee\` decimal(13,2) DEFAULT NULL COMMENT 'i_商品运费',
  \`mct_id\` int DEFAULT NULL COMMENT 'i_商家ID',
  \`comment_cnt_6m\` int DEFAULT NULL COMMENT 'i_近180天评论数',
  \`comment_good_cnt_6m\` int DEFAULT NULL COMMENT 'i_近180天好评数',
  \`comment_bad_cnt_6m\` int DEFAULT NULL COMMENT 'i_近180天差评数',
  \`gmv_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天gmv',
  \`gmv_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天gmv',
  \`gmv_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天gmv',
  \`sales_vol_1w\` int DEFAULT NULL COMMENT 'i_近7天销量',
  \`sales_vol_15d\` int DEFAULT NULL COMMENT 'i_近15天销量',
  \`sales_vol_1m\` int DEFAULT NULL COMMENT 'i_近30天销量',
  \`expre_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天曝光数',
  \`expre_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天曝光数',
  \`expre_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天曝光数',
  \`clk_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天点击数',
  \`clk_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天点击数',
  \`clk_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天点击数',
  \`collect_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天收藏数',
  \`collect_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天收藏数',
  \`collect_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天收藏数',
  \`add_cat_cnt_1w\` int DEFAULT NULL COMMENT 'i_近7天加购数',
  \`add_cat_cnt_15d\` int DEFAULT NULL COMMENT 'i_近15天加购数',
  \`add_cat_cnt_1m\` int DEFAULT NULL COMMENT 'i_近30天加购数',
  \`clk_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天点击率',
  \`clk_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天点击率',
  \`clk_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天点击率',
  \`pay_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天支付转换率',
  \`pay_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天支付转换率',
  \`pay_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天支付转换率',
  \`add_cat_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天加购转化率',
  \`add_cat_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天加购转化率',
  \`add_cat_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天加购转化率',
  \`cr_rate_1w\` decimal(13,2) DEFAULT NULL COMMENT 'i_近7天转换率',
  \`cr_rate_15d\` decimal(13,2) DEFAULT NULL COMMENT 'i_近15天转换率',
  \`cr_rate_1m\` decimal(13,2) DEFAULT NULL COMMENT 'i_近30天转换率',
  \`goods_id\` int(11) DEFAULT NULL COMMENT 'i_商品ID',
  \`goods_name\` varchar(512) DEFAULT NULL COMMENT 'i_商品名称',
  \`goods_sn\` varchar(50) DEFAULT NULL COMMENT 'i_商品所属sn',
  \`is_on_sale\` int(2) DEFAULT NULL COMMENT 'i_真实是否在售',
  \`is_recommend\` int(2) DEFAULT NULL COMMENT 'i_是否可推荐',
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`idx_gs_id\` (\`gs_id\`),
  KEY \`idx_second_cat_id\` (\`second_cat_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品画像表';
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=3000 \
-Dmapreduce.map.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table ads_goods_portrait_now \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_portrait \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns  gs_id,cat_id,first_cat_id,second_cat_id,brand_id,shop_price,gs_discount,shipping_fee,mct_id,comment_cnt_6m,comment_good_cnt_6m,comment_bad_cnt_6m,gmv_1w,gmv_15d,gmv_1m,sales_vol_1w,sales_vol_15d,sales_vol_1m,expre_cnt_1w,expre_cnt_15d,expre_cnt_1m,clk_cnt_1w,clk_cnt_15d,clk_cnt_1m,collect_cnt_1w,collect_cnt_15d,collect_cnt_1m,add_cat_cnt_1w,add_cat_cnt_15d,add_cat_cnt_1m,clk_rate_1w,clk_rate_15d,clk_rate_1m,pay_rate_1w,pay_rate_15d,pay_rate_1m,add_cat_rate_1w,add_cat_rate_15d,add_cat_rate_1m,cr_rate_1w,cr_rate_15d,cr_rate_1m,goods_id,goods_name,goods_sn,is_on_sale,is_recommend \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_goods_portrait to themis.ads_goods_portrait_pre,themis.ads_goods_portrait_now to themis.ads_goods_portrait;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
