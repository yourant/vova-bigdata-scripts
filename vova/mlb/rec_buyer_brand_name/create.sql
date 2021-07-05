create external TABLE mlb.mlb_vova_search_words_top
(
    brand_name   string COMMENT 'brand_name',
    rn       bigint COMMENT 'rn'
) COMMENT 'mlb_vova_search_words_top'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

create external TABLE mlb.mlb_vova_brand_like
(
    buyer_id int COMMENT 'brand_id',
    brand_name   string COMMENT 'brand_name',
    rn int COMMENT 'rn'
) COMMENT 'mlb_vova_brand_like'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;