DROP TABLE ads.ads_vova_web_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_goods
(
    goods_id    bigint
) COMMENT '#9121 vova网站召回池调整' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS themis.ads_vova_web_goods
(
    id                   int(11)          NOT NULL AUTO_INCREMENT,
    goods_id             int(11) UNSIGNED NOT NULL COMMENT '商品id',
    create_time          timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time     timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='#9121 vova网站召回池调整'
;


DROP TABLE tmp.tmp_ads_vova_web_goods_210407;
CREATE TABLE tmp.tmp_ads_vova_web_goods_210407
(
    goods_id         bigint
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
;

load data local inpath '/home/hadoop/vova_web_goods_210407.txt' into table tmp.tmp_ads_vova_web_goods_210407;

select count(*) as a,goods_id from tmp.tmp_ads_vova_web_goods_210407
group by goods_id
having a > 1
;

INSERT OVERWRITE TABLE ads.ads_vova_web_goods PARTITION (pt = '2021-04-06')
SELECT
/*+ REPARTITION(1) */
DISTINCT goods_id
FROM tmp.tmp_ads_vova_web_goods_210407
;

select pt,count(*),count(distinct goods_id) from ads.ads_vova_web_goods group by pt;

