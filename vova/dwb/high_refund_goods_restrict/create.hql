[5742]高退款率屏蔽商品监控报表
https://zt.gitvv.com/index.php?m=task&f=view&taskID=22435

任务描述
需求链接：https://docs.google.com/spreadsheets/d/1LYvyK43igigMdcqn9XXD_F6cyC_hohldikYzDF823Is/edit#gid=0


需求
监控产品需求5091中，符合条件的被屏蔽商品GMV总量，以及每日新增屏蔽商品数量和GMV变化

字段:
日期
累计屏蔽商品总数
每日新增商品数
新增屏蔽商品日均GMV            新增屏蔽商品过去14天GMV/14
屏蔽商品占总商品数比例         累计屏蔽商品总数/总商品数

高退款率屏蔽商品监控

ads.ads_goods_restrict_d

DROP TABLE IF EXISTS dwb.dwb_vova_high_refund_goods_restrict_monitor;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_high_refund_goods_restrict_monitor
(
restrict_goods_cnt         bigint         COMMENT '累计屏蔽商品总数',
new_add_restrict_goods_cnt bigint         COMMENT '每日新增屏蔽商品数',
gmv_day14                  decimal(16, 4) COMMENT '新增屏蔽商品过去14天日均GMV',
goods_cnt                  bigint         COMMENT '总商品数',

nlrf_rate_gt_20_goods_cnt  int            comment '非物流退款率＞20%的商品数',
sales_goods_cnt            int            comment '有销量的商品数',
restrict_gsn_cnt           int            comment '当日新增屏蔽的（id为0的GSN数及SN数量）',
restrict_sold_out_gsn_cnt  int            comment '当日新增屏蔽的的gsn中在架商品数为0的gsn数',
high_nlrf_goods_rate       decimal(10, 4) comment '高退款率商品占比',
restrict_gsn_rate          decimal(10, 4) comment '当日屏蔽商品跟卖率'
) COMMENT '高退款率屏蔽商品监控报表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor/"
;

高退款率屏蔽商品监控
2021-01-23 历史数据迁移
dwb.dwb_vova_high_refund_goods_restrict_monitor;

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_high_refund_goods_restrict_monitor/*

hadoop distcp -overwrite -m 30 hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_high_refund_goods_restrict_monitor/  s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_high_refund_goods_restrict_monitor/

msck repair table dwb.dwb_vova_high_refund_goods_restrict_monitor;
select * from dwb.dwb_vova_high_refund_goods_restrict_monitor limit 20;

# */
###### 20210303 [8583]高退款率屏蔽商品监控报表修改
https://zt.gitvv.com/index.php?m=task&f=view&taskID=31744

在 [5742]高退款率屏蔽商品监控报表 上添加字段
需求内容：
新增两个字段。
https://docs.google.com/spreadsheets/d/1kTZOaT2jXKhb0SPkHInM_rTx6yQD0cUxrRUfS_Gk9TQ/edit#gid=1903628281

高退款率商品占比: 9周非物流退款率＞20%的商品数/有销量的商品数
  非物流退款率＞20%的商品数
  有销量的商品数
当日屏蔽商品跟卖率	当日新增屏蔽的的gsn中在架商品数为0的gsn数/当日新增屏蔽的（id为0的GSN数及SN数量）

create table dwb.dwb_vova_goods_gsn
(
  goods_id    int     COMMENT '商品ID',
  goods_sn    string  COMMENT '显示商品单号，商家克隆商品',
  is_on_sale  int     COMMENT '真实是否在售,1:已上架，0：已下架'
) COMMENT '高退款率屏蔽商品历史gsn' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;

alter table dwb.dwb_vova_high_refund_goods_restrict_monitor add columns(nlrf_rate_gt_20_goods_cnt int comment '非物流退款率＞20%的商品数') cascade;
alter table dwb.dwb_vova_high_refund_goods_restrict_monitor add columns(sales_goods_cnt int comment '有销量的商品数') cascade;
alter table dwb.dwb_vova_high_refund_goods_restrict_monitor add columns(restrict_gsn_cnt int comment '当日新增屏蔽的（id为0的GSN数及SN数量）') cascade;
alter table dwb.dwb_vova_high_refund_goods_restrict_monitor add columns(restrict_sold_out_gsn_cnt int comment '当日新增屏蔽的的gsn中在架商品数为0的gsn数') cascade;

alter table dwb.dwb_vova_high_refund_goods_restrict_monitor add columns(high_nlrf_goods_rate decimal(10, 4) comment '高退款率商品占比') cascade;
alter table dwb.dwb_vova_high_refund_goods_restrict_monitor add columns(restrict_gsn_rate decimal(10, 4) comment '当日屏蔽商品跟卖率') cascade;


DROP TABLE IF EXISTS dwb.dwb_vova_high_refund_goods_restrict_today;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_high_refund_goods_restrict_today
(
  restrict_goods_cnt         bigint         COMMENT '累计屏蔽商品总数',
  new_add_restrict_goods_cnt bigint         COMMENT '每日新增屏蔽商品数',
  gmv_day14                  decimal(16, 4) COMMENT '新增屏蔽商品过去14天日均GMV',
  goods_cnt                  bigint         COMMENT '总商品数',
  nlrf_rate_gt_20_goods_cnt  int            COMMENT '非物流退款率＞20%的商品数',
  sales_goods_cnt            int            COMMENT '有销量的商品数',
  restrict_gsn_cnt           int            COMMENT '当日新增屏蔽的（id为0的GSN数及SN数量）'
) COMMENT '高退款率屏蔽商品监控每天结果' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;





