[8523]Tik Tok渠道用户推荐优化
https://zt.gitvv.com/index.php?m=task&f=view&taskID=31589
任务描述
需求背景：
针对Tik Tok用户进行推荐优化，提高该渠道用户的转化率
需求内容：
1.实时获取用户的渠道来源，数据通过消息队列获取用户的渠道来源，供调用

2.新增召回：
a.统计Tik Tok渠道top5国家，每个国家/全站各取销量最高top200商品，根据用户的国家获取相应的商品
b.统计0~18和18~35年龄段用户，各取两个段内销量最高的top100商品

3.对于来着Tik Tok渠道的用户，首页加入以上a/b两路召回，共400个商品，用于排序。
标记位分别为9/10，详见：https://docs.google.com/spreadsheets/d/1ehIahl7JloD2HOadRyl6PadD0L0UGtMxYDs_UhYpFms/edit#gid=0

仅用于vova

# 2021-02-27
drop table ads.ads_vova_buyer_channel;
create table ads.ads_vova_buyer_channel (
  device_id         string  comment '用户当前设备Id',
  main_channel      string  comment '主渠道',
  child_channel     string  comment '子渠道',
  channel           string  comment '渠道，可以重新赋值' -- 不确定会不会更换main_channel，or child_channel, 当前使用child_channel
) COMMENT '用户渠道'
PARTITIONED BY (pt string)
STORED AS PARQUETFILE
;

select *
from
(
    select device_id,
           count(*) cnt
    from ads.ads_vova_buyer_channel
    group by device_id
) where cnt > 1
limit 20
;

-- #离线更新一次
insert overwrite table ads.ads_vova_buyer_channel partition(pt='2021-02-26')
select /*+ REPARTITION(6) */
  dd.device_id device_id,
  dd.main_channel main_channel,
  dd.child_channel child_channel,
  dd.child_channel channel
from
  dim.dim_vova_devices dd
where dd.datasource = 'vova'
  and dd.device_id is not null
  and dd.main_channel is not null
;

hadoop fs -du -s -h s3://bigdata-offline/warehouse/ads/ads_vova_buyer_channel/pt=2021-02-26/

# mysql 离线任务获取用户渠道历史数据
create table ads_vova_buyer_channel (
  id            int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  device_id     varchar(150)  NOT NULL COMMENT '设备id',
  channel       varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '渠道',
  update_time   datetime      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY device_id (device_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='vova用户渠道';


2.新增召回：

a.统计Tik Tok渠道top5国家，每个国家/全站各取销量最高top200商品，根据用户的国家获取相应的商品
drop table ads.ads_vova_channel_region_top;
create table ads.ads_vova_channel_region_top (
  goods_id          int     comment '商品id',
  region_id         int     comment 'region_id，默认值-1代表全站',
  region_code       string  comment 'region code',
  goods_number      int     comment '销量',
  channel           string  comment '渠道'
) COMMENT 'Tik Tok渠道top5国家，每个国家/全站各取销量最高top200商品'
PARTITIONED BY (pt string)
STORED AS PARQUETFILE
;

# mysql
CREATE TABLE `ads_vova_channel_region_top`  (
    `id`           int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
    `channel`      varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '渠道',
    `region_id`    int(11) NOT NULL DEFAULT -1 COMMENT 'region_id，默认值-1代表全站',
    `goods_id`     int(11) NOT NULL COMMENT '商品id',
    `create_time`  timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
    PRIMARY KEY (`id`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = 'Tik Tok渠道top5国家，每个国家/全站各取销量最高top200商品' ROW_FORMAT = Dynamic;



b.统计0~18和18~35年龄段用户，各取两个段内销量最高的top100商品
drop table ads.ads_vova_age_range_top;
create table ads.ads_vova_age_range_top (
  goods_id          int     comment '商品id',
  age_range         string  comment '所属年龄段'
) COMMENT '统计0~18和18~35年龄段用户，各取两个段内销量最高的top100商品'
PARTITIONED BY (pt string)
STORED AS PARQUETFILE
;


CREATE TABLE `ads_vova_age_range_top`  (
    `id`          int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
    `age_range`   varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '所属年龄段',
    `goods_id`    int(11) NOT NULL COMMENT '商品id',
    `create_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
    PRIMARY KEY (`id`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '统计0~18和18~35年龄段用户，各取两个段内销量最高的top100商品' ROW_FORMAT = Dynamic;












