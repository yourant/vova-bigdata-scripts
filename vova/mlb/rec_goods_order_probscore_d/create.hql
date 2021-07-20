[10012] 商详页上线商品成交概率分ab实验-导数

https://confluence.gitvv.com/pages/viewpage.action?pageId=21281091
商品成交概率分模型

一、模型所用特征
见谷歌文档：https://docs.google.com/spreadsheets/d/1gbG4BOJkCszfbpa_WQuDvN1pmyaVR4eJnXRRaG8WjlA/edit#gid=0

模型采用LightGBM 进行建模， 样本为近一个月有点击商品的相关特征，标签取当天是否有加购和购买作为label

二、模型评分输出结果
模型打分表，建表语句如下：

CREATE external table mlb.mlb_vova_rec_goods_order_probscore_d(
goods_id             bigint        COMMENT '商品id',
first_cat_id         bigint        COMMENT '一级品类',
second_cat_id        bigint        COMMENT '二级品类',
third_cat_id         bigint        COMMENT '三级品类',
fourth_cat_id        bigint        COMMENT '四级品类',
is_brand             bigint        COMMENT '是否brand',
order_probscore      decimal(10,4) COMMENT '成交概率分'
)COMMENT '商品成交概率分结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS textfile
LOCATION "s3://vova-mlb/REC/data/base/mlb_vova_rec_goods_order_probscore_d/";


create table rec_recall.mlb_vova_rec_goods_order_probscore_d(
  id               int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id         int(11) NOT NULL  COMMENT '商品id',
  first_cat_id     int(11) NOT NULL  COMMENT '一级品类',
  second_cat_id    int(11) DEFAULT 0 COMMENT '二级品类',
  third_cat_id     int(11) DEFAULT 0 COMMENT '三级品类',
  fourth_cat_id    int(11) DEFAULT 0 COMMENT '四级品类',
  is_brand         int(11) DEFAULT 0 COMMENT '是否brand',
  order_probscore  double  NOT NULL  COMMENT '成交概率分',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE,
  KEY idx_first_score_goods (first_cat_id, order_probscore, goods_id),
  KEY idx_second_score_goods (second_cat_id, order_probscore, goods_id),
  KEY idx_third_score_goods (third_cat_id, order_probscore, goods_id),
  KEY idx_fourth_score_goods (fourth_cat_id, order_probscore, goods_id),
  KEY idx_score_goods_brand (order_probscore, goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品成交概率分结果表';