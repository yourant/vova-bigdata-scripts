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
goods_cnt                  bigint         COMMENT '总商品数'
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

#
