优质商家类目降级监控

需求方及需求号:
创建时间及开发人员：2020-12-02,陈凯
修改需求方及需求号:
修改人及修改时间:

# ads.ads_mct_rank
ads.ads_vova_mct_rank

Drop table dwb.dwb_vova_mct_first_cat_rank_downgrading;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_mct_first_cat_rank_downgrading (
datasource            string         COMMENT 'd_datasource',
spsor_name            string         COMMENT 'i_招商经理',
mct_id                bigint         COMMENT 'd_商家ID',
mct_name              string         COMMENT 'd_商家名称',
terday_rank           bigint         COMMENT 'i_前一日等级',
today_rank            bigint         COMMENT 'i_当日等级',
first_cat_id          string         COMMENT 'd_一级品类ID',
first_cat_name        string         COMMENT 'd_一级品类名称'
) COMMENT '优质商家类目降级监控' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/"
;

优质商家类目降级监控
2021-01-23 历史数据迁移
dwb.dwb_vova_mct_first_cat_rank_downgrading

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_mct_first_cat_rank_downgrading/*

hadoop distcp -overwrite -m 30 hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_mct_first_cat_rank_downgrading/  s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/

msck repair table dwb.dwb_vova_mct_first_cat_rank_downgrading;
select * from dwb.dwb_vova_mct_first_cat_rank_downgrading limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_mct_first_cat_rank_downgrading/pt=2021-0*
