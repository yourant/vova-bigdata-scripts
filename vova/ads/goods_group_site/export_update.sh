#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists trigram_data.ads_goods_behave_group_site_pre;
drop table if exists trigram_data.ads_goods_behave_group_site_new;
CREATE TABLE IF NOT EXISTS \`trigram_data\`.\`ads_goods_behave_group_site_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`vir_goods_id\` int(11) NOT NULL COMMENT '虚拟商品id',
  \`commodity_id\` varchar(100)  COMMENT 'commodity_id',
  \`project_name\` varchar(100)  COMMENT 'project_name',
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`platform\` varchar(10) NOT NULL COMMENT '终端类型，pc、h5',
  \`expre_cnt\` int(11) NOT NULL COMMENT '近七日曝光量',
  \`clk_cnt\`   int(11) NOT NULL COMMENT '近七日点击量',
  \`order_cnt\`   int(11) NOT NULL COMMENT '近七日订单量',
  \`sales_vol\`   int(11) NOT NULL COMMENT '近日期销量',
  \`expre_uv\`    int(11) NOT NULL COMMENT '近七日曝光uv',
  \`clk_uv\`      int(11) NOT NULL COMMENT '近七日点击uv',
  \`add_cat_uv\`  int(11) NOT NULL COMMENT '近七日加车uv',
  \`order_uv\`    int(11) NOT NULL COMMENT '近七日下单uv',
  \`pay_uv\`      int(11) NOT NULL COMMENT '近七日支付uv',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id_key\` (\`goods_id\`),
  KEY \`platform_key\` (\`platform\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`trigram_data\`.\`ads_goods_behave_group_site\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`vir_goods_id\` int(11) NOT NULL COMMENT '虚拟商品id',
  \`commodity_id\` varchar(100)  COMMENT 'commodity_id',
  \`project_name\` varchar(100)  COMMENT 'project_name',
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`platform\` varchar(10) NOT NULL COMMENT '终端类型，pc、h5',
  \`expre_cnt\` int(11) NOT NULL COMMENT '近七日曝光量',
  \`clk_cnt\`   int(11) NOT NULL COMMENT '近七日点击量',
  \`order_cnt\`   int(11) NOT NULL COMMENT '近七日订单量',
  \`sales_vol\`   int(11) NOT NULL COMMENT '近日期销量',
  \`expre_uv\`    int(11) NOT NULL COMMENT '近七日曝光uv',
  \`clk_uv\`      int(11) NOT NULL COMMENT '近七日点击uv',
  \`add_cat_uv\`  int(11) NOT NULL COMMENT '近七日加车uv',
  \`order_uv\`    int(11) NOT NULL COMMENT '近七日下单uv',
  \`pay_uv\`      int(11) NOT NULL COMMENT '近七日支付uv',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`goods_id_key\` (\`goods_id\`),
  KEY \`platform_key\` (\`platform\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"
mysql -h trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com -u trigram_data2021052811 -piPha7Sae5Quai3ahR5vi~ -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com:3306/trigram_data \
--username trigram_data2021052811 --password iPha7Sae5Quai3ahR5vi~ \
--m 1 \
--table ads_goods_behave_group_site_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_behave_group_site \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns vir_goods_id,commodity_id,project_name,goods_id,platform,expre_cnt,clk_cnt,order_cnt,sales_vol,expre_uv,clk_uv,add_cat_uv,order_uv,pay_uv \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h trigram-fn.cn899du7tges.us-east-1.rds.amazonaws.com -u trigram_data2021052811 -piPha7Sae5Quai3ahR5vi~ <<EOF
rename table trigram_data.ads_goods_behave_group_site to trigram_data.ads_goods_behave_group_site_pre,trigram_data.ads_goods_behave_group_site_new to trigram_data.ads_goods_behave_group_site;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi