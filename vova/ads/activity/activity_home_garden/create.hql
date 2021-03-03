[7369]家居会场接入算法服务
https://zt.gitvv.com/index.php?m=task&f=view&taskID=27680
需求背景：

家居会场接入算法，提高家居会场的个性化程度和更新频率。

需求描述：

1. 数据计算：
详细见：https://docs.google.com/spreadsheets/d/1Sv03zGjeYa2UIn27pSnTSo-wrHaWozCSUr4h3_fWzA4/edit?usp=sharing

2. 服务输出：
限制只出非brand商品。
通过活动接口输出，每个会场均为一个单独的全品类接口，用biztype标记数据源。
每次请求时，取出所有符合条件的数据，按照指定排序后输出。
保留过滤和屏蔽逻辑、保留相似商品组取低价逻辑。
每次刷新过滤已输出商品，保证刷新可变。

CREATE TABLE IF NOT EXISTS `themis`.`ads_activity_home_garden` (
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
    KEY `region_id_key` (`region_id`),
    KEY `biz_type_key` (`biz_type`),
    KEY `rp_type_key` (`rp_type`),
    KEY `first_cat_id_key` (`first_cat_id`),
    KEY `second_cat_id_key` (`second_cat_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='家居会场';


drop table if exists ads.ads_vova_activity_home_garden;
create table if not exists ads.ads_vova_activity_home_garden (
    `goods_id`                  int    COMMENT '商品id',
    `region_id`                 int    COMMENT '国家id',
    `first_cat_id`              int    COMMENT '一级品类id',
    `second_cat_id`             int    COMMENT '二级品类id',
    `biz_type`                  string COMMENT 'biz_type,规则id',
    `rp_type`                   string COMMENT 'rp标记',
    `rank`                      int    COMMENT '排名'
) COMMENT '家居会场活动表' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE;







