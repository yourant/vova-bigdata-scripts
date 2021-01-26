[6571]省钱月卡报表一期
https://zt.gitvv.com/index.php?m=task&f=view&taskID=25305

[7070]省钱月卡报表优化v1.1
https://zt.gitvv.com/index.php?m=task&f=view&taskID=26628

# ads.ads_buyer_gmv_stage_3m 可以换成 ads.ads_buyer_portrait_feature
ads.ads_vova_buyer_portrait_feature
(select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}')

表一：购卡链路转化率监控
drop table if exists dwb.dwb_vova_bonus_card_conversion;
CREATE external TABLE  IF NOT EXISTS dwb.dwb_vova_bonus_card_conversion
(
    datasource                  string COMMENT 'd_datasource',
    region_code                 string COMMENT 'd_国家',
    os_type                     string COMMENT 'd_平台',
    main_channel                string COMMENT 'd_渠道',
    is_new                      string COMMENT 'd_激活时间',
    gmv_stage                   string COMMENT 'd_用户等级',
    dau                         bigint COMMENT 'i_非月卡DAU',
    vouchercard_hp_unpay_uv     bigint COMMENT 'i_未购卡会场页面UV(非pending)',
    payment_bonus_card_uv       bigint COMMENT 'i_月卡支付页面UV',
    paid_uv                     bigint COMMENT 'i_开卡成功UV',
    pending_uv                  bigint COMMENT 'i_开卡pendingUV'
) COMMENT '购卡链路转化率监控'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion/"
;

表二：优惠券发放与核销
drop table if exists dwb.dwb_vova_bonus_card_coupon;
CREATE external TABLE  IF NOT EXISTS dwb.dwb_vova_bonus_card_coupon
(
    datasource         string COMMENT 'd_datasource',
    region_code        string COMMENT 'd_国家',
    os_type            string COMMENT 'd_平台',
    gmv_stage          string COMMENT 'd_用户等级',
    cpn_cfg_type_id    string COMMENT 'd_优惠券type_id',
    cpn_cfg_type_name  string COMMENT 'd_优惠券type_name',
    get_cpn_cnt        bigint COMMENT 'i_优惠券已领数量',
    get_cpn_user_cnt   bigint COMMENT 'i_优惠券已领人数',
    get_cpn_sum        decimal(13,4) COMMENT 'i_优惠券发放金额',
    use_cpn_cnt        bigint COMMENT 'i_优惠券使用数量',
    use_cpn_user_cnt   bigint COMMENT 'i_优惠券使用人数',
    use_cpn_sum        decimal(13,4) COMMENT 'i_优惠券使用金额',
    cpn_order_gmv      decimal(13,4) COMMENT 'i_优惠券带来GMV'
) COMMENT '优惠券发放与核销'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon/"
;

表三：月卡用户交易数据
drop table if exists dwb.dwb_vova_bonus_card_pay;
CREATE external TABLE  IF NOT EXISTS dwb.dwb_vova_bonus_card_pay
(
    datasource                  string COMMENT 'd_datasource',
    region_code                 string COMMENT 'd_国家',
    os_type                     string COMMENT 'd_平台',
    main_channel                string COMMENT 'd_渠道',
    is_new                      string COMMENT 'd_激活时间',
    gmv_stage                   string COMMENT 'd_用户等级',

    bonus_card_paid_dau         bigint COMMENT 'i_月卡用户DAU',
    bonus_card_pending_dau      bigint COMMENT 'i_月卡pending用户DAU',
    bonus_card_gmv              decimal(13,4) COMMENT 'i_月卡用户GMV',
    bonus_card_order_cnt        bigint COMMENT 'i_月卡用户支付成功订单量',
    bonus_card_first_order_cnt  bigint COMMENT 'i_月卡用户首单订单量',
    bonus_card_device_cnt       bigint COMMENT 'i_月卡用户支付成功UV',
    bonus_card_price            decimal(13,4) COMMENT 'i_开卡费用总和',
    gmv                         decimal(13,4) COMMENT 'i_大盘GMV',
    cpn_cfg_val                 decimal(13,4) COMMENT 'i_月卡优惠券抵扣金额'
) COMMENT '月卡用户交易数据'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay/"
;

表四：主页留存数据监控
drop table if exists dwb.dwb_vova_vouchercard_hp_paid_retention;
CREATE external TABLE  IF NOT EXISTS dwb.dwb_vova_vouchercard_hp_paid_retention
(
    datasource                  string COMMENT 'd_datasource',
    region_code                 string COMMENT 'd_国家',
    os_type                     string COMMENT 'd_平台',
    main_channel                string COMMENT 'd_渠道',
    is_new                      string COMMENT 'd_激活时间',
    gmv_stage                   string COMMENT 'd_用户等级',

    bonus_card_paid_dau         bigint COMMENT 'i_月卡DAU',
    vouchercard_hp_paid_uv      bigint COMMENT 'i_已购卡会场页面UV',
    renew_button_click_uv       bigint COMMENT 'i_续费按钮点击UV',
    push_switch_open_uv         bigint COMMENT 'i_推送开关打开UV',
    paid_goods_impression_pv    bigint COMMENT 'i_已购卡商品列表曝光数pv',
    paid_goods_click_pv         bigint COMMENT 'i_已购卡商品列表点击数pv',
    gmv                         decimal(13,4) COMMENT 'i_会场商品归因GMV',
    vouchercard_hp_paid_day2_uv bigint COMMENT 'i_当日浏览uv中在次日浏览的uv',
    vouchercard_hp_paid_day7_uv bigint COMMENT 'i_当日浏览uv中在第七日浏览的uv',
    vouchercard_hp_paid_day14_uv bigint COMMENT 'i_当日浏览uv中在第14日浏览的uv',
    vouchercard_hp_paid_day28_uv bigint COMMENT 'i_当日浏览uv中在第28日浏览的uv',

    paid_app_uv        bigint COMMENT 'i_已购卡appUV',
    paid_app_day2_uv   bigint COMMENT 'i_当日浏览uv中在次日浏览的uv',
    paid_app_day7_uv   bigint COMMENT 'i_当日浏览uv中在第七日浏览的uv',
    paid_app_day14_uv  bigint COMMENT 'i_当日浏览uv中在第14日浏览的uv',
    paid_app_day28_uv  bigint COMMENT 'i_当日浏览uv中在第28日浏览的uv'
) COMMENT '主页留存数据监控'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention/"
;

-- 2020-11-20 新增字段
alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(vouchercard_hp_paid_day14_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN vouchercard_hp_paid_day14_uv vouchercard_hp_paid_day14_uv  bigint comment 'i_当日浏览uv中在第14日浏览的uv';


alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(vouchercard_hp_paid_day28_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN vouchercard_hp_paid_day28_uv vouchercard_hp_paid_day28_uv  bigint comment 'i_当日浏览uv中在第28日浏览的uv';

-- 2020-11-24 新增字段
-- paid_app_uv
-- paid_app_day2_uv
-- paid_app_day7_uv
-- paid_app_day14_uv
-- paid_app_day28_uv
alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(paid_app_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN paid_app_uv paid_app_uv  bigint comment 'i_已购卡appUV';

alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(paid_app_day2_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN paid_app_day2_uv paid_app_day2_uv  bigint comment 'i_当日浏览uv中在次日浏览的uv';

alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(paid_app_day7_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN paid_app_day7_uv paid_app_day7_uv  bigint comment 'i_当日浏览uv中在第7日浏览的uv';

alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(paid_app_day14_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN paid_app_day14_uv paid_app_day14_uv  bigint comment 'i_当日浏览uv中在第14日浏览的uv';

alter table dwb.dwb_vova_vouchercard_hp_paid_retention add columns(paid_app_day28_uv bigint) cascade;
alter table dwb.dwb_vova_vouchercard_hp_paid_retention CHANGE COLUMN paid_app_day28_uv paid_app_day28_uv  bigint comment 'i_当日浏览uv中在第28日浏览的uv';


省钱月卡
2021-01-23 历史数据迁移
dwb.dwb_vova_bonus_card_conversion

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_bonus_card_conversion/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_bonus_card_conversion/  s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_conversion/

msck repair table dwb.dwb_vova_bonus_card_conversion;
select * from dwb.dwb_vova_bonus_card_conversion limit 20;



2021-01-23 历史数据迁移
dwb.dwb_vova_bonus_card_coupon

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_bonus_card_coupon/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_bonus_card_coupon/  s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_coupon/

msck repair table dwb.dwb_vova_bonus_card_coupon;
select * from dwb.dwb_vova_bonus_card_coupon limit 20;

2021-01-23 历史数据迁移
dwb.dwb_vova_bonus_card_pay

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_bonus_card_pay/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_bonus_card_pay/  s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_bonus_card_pay/

msck repair table dwb.dwb_vova_bonus_card_pay;
select * from dwb.dwb_vova_bonus_card_pay limit 20;

2021-01-23 历史数据迁移
dwb.dwb_vova_vouchercard_hp_paid_retention

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_vouchercard_hp_paid_retention/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_vouchercard_hp_paid_retention/  s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_vouchercard_hp_paid_retention/

msck repair table dwb.dwb_vova_vouchercard_hp_paid_retention;
select * from dwb.dwb_vova_vouchercard_hp_paid_retention limit 20;

