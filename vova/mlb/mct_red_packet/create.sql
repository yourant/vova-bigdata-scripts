[8307]商家红包
https://zt.gitvv.com/index.php?m=my&f=task&type=assignedTo

任务描述
需求背景：近期会新上线两个活动：商家红包活动和春节不打烊活动，因此需要开发若干商品池来支持
需求内容：https://docs.google.com/spreadsheets/d/1JAj-Tpy1MOYhfhYBkbBDlBynDPDsSb9PyB_MWB3hoZI/edit#gid=0

依赖：
[6662]降价商品模块新增红包逻辑
[7164]红包商品曝光量问题过少取数排查

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki
use themis;

select * from ads_lower_price_goods_red_packet where is_invalid = 0 and is_delete = 0 and red_packet_cnt > 0 limit 20;

从 降价商品模块新增红包逻辑项目中输出的mysql 表 ads_lower_price_goods_red_packet 中
取 商品组中红包个数最大的商品
输出 一个红包活动表

# 需要使用 rename 方式 每小时更新

CREATE TABLE IF NOT EXISTS `themis`.`ads_mct_red_packet` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `region_id` int(11) NOT NULL COMMENT '国家id',
  `first_cat_id` int(11) NOT NULL COMMENT '一级品类id',
  `second_cat_id` int(11) NOT NULL COMMENT '二级品类id',
  `biz_type` varchar(50) NOT NULL COMMENT 'biz_type,规则id',
  `rp_type` varchar(10) NOT NULL COMMENT 'rp标记',
  `rank` int(11) NOT NULL COMMENT '序号',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `goods_id` (`goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商家红包';


