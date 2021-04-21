create external table mlb.mlb_vova_search_correct_word_d
(
   word   string  comment '词'
) comment '搜索词'
PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/search/correct/mlb_vova_search_correct_word_d"
;

create external table mlb.mlb_vova_search_hard_correct_d
(
   src_word   string comment '原始词',
   word       string comment '纠正词'
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as textfile
LOCATION "s3://vova-mlb/REC/data/search/correct/mlb_vova_search_hard_correct_d"
;

create table mlb.mlb_vova_search_correct_gram_d
(
   src_word   string  comment '前词',
   dist_word  string  comment '后词',
   molecular  bigint  comment '概率分子',
   denominator bigint comment '概率分母',
   prob       double  comment '概率'
) comment '搜索词'
PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/search/correct/mlb_vova_search_correct_gram_d"
;


ALTER TABLE themis.mlb_vova_search_correct_word_d MODIFY word
varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL

CREATE TABLE themis.mlb_vova_search_correct_word_d (
  id int(11) NOT NULL AUTO_INCREMENT,
  word varchar(128) NOT NULL COMMENT '正确、不需要纠错的单词',
  PRIMARY KEY (id),
  KEY idx_word (word) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


ALTER TABLE themis.mlb_vova_search_hard_correct_d MODIFY src_word
varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL

CREATE TABLE themis.mlb_vova_search_hard_correct_d (
  id int(11) NOT NULL AUTO_INCREMENT,
  src_word varchar(128) DEFAULT NULL COMMENT '原词',
  word varchar(255) DEFAULT NULL COMMENT '用于替换原词的单词',
  PRIMARY KEY (id),
  KEY idx_src_word (src_word) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE themis.mlb_vova_search_correct_gram_d MODIFY src_word
varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL;
ALTER TABLE themis.mlb_vova_search_correct_gram_d MODIFY dist_word
varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL;

CREATE TABLE themis.mlb_vova_search_correct_gram_d (
  id int(11) NOT NULL AUTO_INCREMENT,
  src_word varchar(128) DEFAULT NULL COMMENT '前词',
  dist_word varchar(128) DEFAULT NULL COMMENT '后词',
  prob float DEFAULT NULL COMMENT '前后词搭配的概率',
  PRIMARY KEY (id),
  KEY idx_src_dist_word (src_word,dist_word) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;