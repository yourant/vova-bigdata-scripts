#!/bin/bash

echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# 需要使用 rename 方式 每小时更新

# 建表
sql="
drop table if exists themis.ads_mct_red_packet_pre;
drop table if exists themis.ads_mct_red_packet_new;

CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_mct_red_packet_new\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '一级品类id',
  \`second_cat_id\` int(11) NOT NULL COMMENT '二级品类id',
  \`biz_type\` varchar(50) NOT NULL COMMENT 'biz_type,规则id',
  \`rp_type\` varchar(10) NOT NULL COMMENT 'rp标记',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商家红包';

CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_mct_red_packet\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`goods_id\` int(11) NOT NULL COMMENT '商品id',
  \`region_id\` int(11) NOT NULL COMMENT '国家id',
  \`first_cat_id\` int(11) NOT NULL COMMENT '一级品类id',
  \`second_cat_id\` int(11) NOT NULL COMMENT '二级品类id',
  \`biz_type\` varchar(50) NOT NULL COMMENT 'biz_type,规则id',
  \`rp_type\` varchar(10) NOT NULL COMMENT 'rp标记',
  \`rank\` int(11) NOT NULL COMMENT '序号',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY \`goods_id\` (\`goods_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商家红包';
"

# bdwriter        | Dd7LvXRPDP4iIJ7FfT8e
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始insert-------"
# 逻辑sql
sql="
insert into themis.ads_mct_red_packet_new
select
  null as id,
  goods_id as goods_id,
  0 as region_id,
  0 as first_cat_id,
  0 as second_cat_id,
  'merchant-coupon' as biz_type,
  '3' as rp_type,
  '0' as rank,
  CURRENT_TIMESTAMP as update_time
from
(
  select
    distinct recommend_goods_id as goods_id
  from
    themis.ads_lower_price_goods_red_packet
  where is_invalid = 0 and is_delete = 0 and red_packet_cnt > 0
) t1
;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_mct_red_packet to themis.ads_mct_red_packet_pre,themis.ads_mct_red_packet_new to themis.ads_mct_red_packet;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi

echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
