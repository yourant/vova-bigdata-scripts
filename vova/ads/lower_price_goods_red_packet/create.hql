[8525]商家红包替换逻辑改为gsn逻辑
https://zt.gitvv.com/index.php?m=task&f=view&taskID=31526

需求背景：
商家红包替换目前按照相似商品组逻辑，需要改成按照gsn逻辑，以提升红包商品的曝光个数和曝光量。

需求描述：
前述需求：6698 降价系统红包算法逻辑


数据：
（1）计算同一GSN下红包库存最大的商品（如有），让服务优先进行红包商品替换，当该商品红包消耗完时，则依次替换下一个红包最多的商品。

服务：
（1）根据数据计算结果在首页、me页、无结果页和列表页等场景优先进行红包替换逻辑，如果没有，则进行低价替换逻辑。（不用改。）

desc ods_vova_vts.ods_vova_gsn_coupon_sign_goods_h;
gcsg_id          bigint          主键id
  goods_id         bigint          报名商品id
  goods_sn         string          goods.goods_sn
  merchant_id      bigint          商家id
  coupon_num       bigint          报名红包数量
  remain_num       bigint          剩余红包数量
  d_value          decimal(13,4)   商品sku价格和目标价格最大差值
  cancel_status    bigint          报名状态,0默认1取消
  operator         string          操作人
  create_time      timestamp       创建时间
  last_update_time timestamp   最后更新时间
  Time taken: 0.068 seconds, Fetched 11 row(s)


ods_vova_vts.ods_vova_gsn_coupon_sign_goods_h
# 根据红包报名表把所有报名商品对应goods_sn下的所有商品取出来,同步到mysql
drop table ads.ads_vova_red_packet_gsn_goods;
CREATE  TABLE IF NOT EXISTS ads.ads_vova_red_packet_gsn_goods
(
    goods_id            int         COMMENT '商品ID',
    goods_sn            string      COMMENT '商品gsn'
) COMMENT '红包报名商品对应gsn下goods_id'
PARTITIONED BY ( pt string)
STORED AS PARQUETFILE;

create table IF NOT EXISTS themis.ads_vova_red_packet_gsn_goods(
  id             int(11)      NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id       bigint       NOT NULL COMMENT '商品id',
  goods_sn       varchar(200) NOT NULL COMMENT '商品GSN',
  update_time    datetime     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='红包报名商品对应gsn下goods_id';
















