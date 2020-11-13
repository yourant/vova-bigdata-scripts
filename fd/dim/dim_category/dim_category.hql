CREATE TABLE IF NOT EXISTS dim.dim_fd_category (
`cat_id` int COMMENT '商品品类id',
`cat_name` string COMMENT '商品品类名',
`depth` int COMMENT '当前叶子深度',
`first_cat_id` int COMMENT '商品一级类目id',
`first_cat_name` string COMMENT '商品一级类目名',
`second_cat_id` int COMMENT '商品二级类目id',
`second_cat_name` string COMMENT '商品二级类目名',
`three_cat_id` int COMMENT '商品三级类目id',
`three_cat_name` string COMMENT '商品三级类目名',
`four_cat_id` int COMMENT '商品四级类目id',
`four_cat_name` string COMMENT '商品四级类目名',
`is_leaf` int COMMENT '是否是叶子节点'
) COMMENT 'category维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

insert overwrite table dim.dim_fd_category
SELECT
c.cat_id,
c.cat_name,
c.depth,
CASE
    WHEN c.parent_id = 0 THEN c.cat_id
    WHEN c_pri.parent_id = 0 THEN c_pri.cat_id
    WHEN c_ga.parent_id = 0 THEN c_ga.cat_id
    WHEN c_th.parent_id = 0 THEN c_th.cat_id
    WHEN c_fou.parent_id = 0 THEN c_fou.cat_id
    END AS first_cat_id,
CASE
    WHEN c.parent_id = 0 THEN c.cat_name
    WHEN c_pri.parent_id = 0 THEN c_pri.cat_name
    WHEN c_ga.parent_id = 0 THEN c_ga.cat_name
    WHEN c_th.parent_id = 0 THEN c_th.cat_name
    WHEN c_fou.parent_id = 0 THEN c_fou.cat_name
    END AS first_cat_name,
CASE
    WHEN c.parent_id = 1 THEN c.cat_id
    WHEN c_pri.parent_id = 1 THEN c_pri.cat_id
    WHEN c_ga.parent_id = 1 THEN c_ga.cat_id
    WHEN c_th.parent_id = 1 THEN c_th.cat_id
    WHEN c_fou.parent_id = 1 THEN c_fou.cat_id
    END AS second_cat_id,
CASE
    WHEN c.parent_id = 1 THEN c.cat_name
    WHEN c_pri.parent_id = 1 THEN c_pri.cat_name
    WHEN c_ga.parent_id = 1 THEN c_ga.cat_name
    WHEN c_th.parent_id = 1 THEN c_th.cat_name
    WHEN c_fou.parent_id = 1 THEN c_fou.cat_name
    END AS second_cat_name,
CASE
    WHEN c_fou.parent_id is not null and c_th.parent_id is not null THEN c_ga.cat_id
    WHEN c_fou.parent_id is null and c_th.parent_id is not null and c_ga.parent_id is not  null then c_pri.cat_id
    else ''
    END AS three_cat_id,
CASE
    WHEN c_fou.parent_id is not null and c_th.parent_id is not null THEN c_ga.cat_name
    WHEN c_fou.parent_id is null and c_th.parent_id is not null and c_ga.parent_id is not  null then c_pri.cat_name
    else ''
    END AS three_cat_name,
CASE
    WHEN c_fou.parent_id is not null THEN c_pri.cat_id
    else ''
    END AS four_cat_id,
CASE
    WHEN c_fou.parent_id is not null THEN c_pri.cat_name
    else ''
    END AS four_cat_name,
CASE
    WHEN c.depth > 0 THEN  1 ELSE 0 END AS is_leaf
FROM (select cat_id,cat_name,depth,parent_id from ods_fd_vb.ods_fd_category) c
LEFT JOIN (select cat_id,cat_name,depth,parent_id from ods_fd_vb.ods_fd_category ) c_pri ON c.parent_id = c_pri.cat_id
LEFT JOIN (select cat_id,cat_name,depth,parent_id from ods_fd_vb.ods_fd_category ) c_ga ON c_pri.parent_id = c_ga.cat_id
LEFT JOIN (select cat_id,cat_name,depth,parent_id from ods_fd_vb.ods_fd_category ) c_th ON c_ga.parent_id = c_th.cat_id
LEFT JOIN (select cat_id,cat_name,depth,parent_id from ods_fd_vb.ods_fd_category ) c_fou ON c_th.parent_id = c_fou.cat_id;
