--品类排序默认数据
drop table if exists ads.ads_vova_category_recommend_default;
create external table if  not exists  ads.ads_vova_category_recommend_default (
    `cat_id`                      int COMMENT 'd_category id',
    `region_id`                   int COMMENT 'd_国家id,0表示全站',
    `gender`                      int COMMENT 'd_1.man，2.wemen，3.unknow',
    `type`                        int COMMENT 'd_1.一级类目，2.二级类目',
    `rank`                        int COMMENT 'i_排名'
) COMMENT '品类排序默认数据'
     STORED AS PARQUETFILE;