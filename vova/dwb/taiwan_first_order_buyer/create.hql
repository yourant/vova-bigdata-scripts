台湾新人首单返券报表
需求方及需求号：吕一方 ,4892
创建时间及开发人员：2020/07/21,陈凯

-- t1 首单返券活动日常报表
drop table dwb.dwb_vova_first_order_coupon;
create external table dwb.dwb_vova_first_order_coupon(
is_new                 string   comment '是否新激活',
region_code            string   comment '国家/地区',
platform               string   comment '平台:ios/android',
no_order_buyer_cnt     bigint   comment '未下单用户数',
dau                    bigint   comment '当日dau',
firstbuyback_dau       bigint   comment '会场总DAU',
firstbuyback_order_cnt bigint   comment '总活动订单数',
firstbuyback_order_gmv bigint   comment '当日活动订单GMV',
order_goods_cnt        bigint   comment '总下单数',
cancel_order_cnt       bigint   comment '取消订单数',
refund_order_goods_cnt bigint   comment '退款子订单数',
refund_order_goods_gmv bigint   comment '退款订单GMV'
) COMMENT '首单返券活动日常报表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_first_order_coupon/"
;


-- 活动订单明细
drop table dwb.dwb_vova_first_order_detail;
create external table dwb.dwb_vova_first_order_detail(
is_new                 string   comment '是否新激活',
region_code            string   comment '国家/地区',
platform               string   comment '平台： ios/android',
firstbuyback_order_cnt bigint   comment  '总活动订单数',
sign_order_cnt         bigint   comment  '主订单里的已签收订单',
refund_order_goods_cnt bigint   comment  '总退款子订单数',
refund_order_goods_gmv bigint   comment  '退款订单GMV',
refund_order_cnt       bigint   comment  '发起退款的子订单所属的父订单数',
firstbuyback_order_gmv bigint   comment  '总活动订单GMV',
order_cnt_0_50         bigint   comment  '0～50元档位订单数',
order_cnt_50_250       bigint   comment  '50～250元档位订单数',
order_cnt_250_1000     bigint   comment  '250~1000元档位订单数',
order_cnt_1000         bigint   comment  '大于1000元档位订单数'
) COMMENT '活动订单明细' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_first_order_detail/"
;


-- 用户核销情况
drop table dwb.dwb_vova_buyer_coupon_use;
create external table dwb.dwb_vova_buyer_coupon_use(
is_new             string              comment '是否新激活',
region_code        string           comment '国家/地区',
platform           string           comment '平台： ios/android',
order_cnt          bigint           comment '主订单里的已签收订单',
use_cpn_order_cnt  bigint           comment '所有使用活动coupon_id的订单数',
                                         use_cpn_order_gmv  DECIMAL(10, 2)   comment '使用活动coupon_id的订单总gmv'
) COMMENT '用户核销情况' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_buyer_coupon_use/"
;


-- 无门槛优惠券核销情况
-- 有门槛优惠券核销情况
drop table dwb.dwb_vova_coupon_use;
create external table dwb.dwb_vova_coupon_use(
is_new               string              comment '是否新激活',
region_code          string           comment '国家/地区',
platform             string           comment '平台： ios/android',
coupon_20_5_cnt      bigint           comment '20-5订单数',
coupon_20_5_gmv      DECIMAL(10, 2)   comment '20-5订单gmv',
coupon_50_10_cnt     bigint           comment '50-10订单数',
coupon_50_10_gmv     DECIMAL(10, 2)   comment '50-10订单gmv',
coupon_40_10_cnt     bigint           comment '40-10订单数',
coupon_40_10_gmv     DECIMAL(10, 2)   comment '40-10订单gmv',
coupon_100_20_cnt    bigint           comment '100-20订单数',
coupon_100_20_gmv    DECIMAL(10, 2)   comment '100-20订单gmv',
coupon_250_50_cnt    bigint           comment '250-50订单数',
coupon_250_50_gmv    DECIMAL(10, 2)   comment '250-50订单gmv',
coupon_400_100_cnt   bigint           comment '400-100订单数',
coupon_400_100_gmv   DECIMAL(10, 2)   comment '400-100订单gmv',
coupon_1000_200_cnt  bigint           comment '1000-200订单数',
coupon_1000_200_gmv  DECIMAL(10, 2)   comment '1000-200订单gmv',
coupon_1_cnt         bigint           comment '无门槛1元订单数',
coupon_1_gmv         DECIMAL(10, 2)   comment '无门槛1元订单gmv',
coupon_2_cnt         bigint           comment '无门槛2元订单数',
coupon_2_gmv         DECIMAL(10, 2)   comment '无门槛2元订单gmv',
coupon_3_cnt         bigint           comment '无门槛3元订单数',
coupon_3_gmv         DECIMAL(10, 2)   comment '无门槛3元订单gmv',
coupon_4_cnt         bigint           comment '无门槛4元订单数',
coupon_4_gmv         DECIMAL(10, 2)   comment '无门槛4元订单gmv',
coupon_5_cnt         bigint           comment '无门槛5元订单数',
coupon_5_gmv         DECIMAL(10, 2)   comment '无门槛5元订单gmv'
) COMMENT '优惠券核销情况' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_coupon_use/"
;
