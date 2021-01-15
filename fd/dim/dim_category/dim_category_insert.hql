insert overwrite table dim.dim_fd_category
select
    /*+ REPARTITION(1) */
    cat_id,
    cat_name,
    depth,
    if(depth=0,cat_id,first_cat_id),
    if(depth=0,cat_name,first_cat_name),
    if(depth=1,cat_id,second_cat_id),
    if(depth=1,cat_name,second_cat_name),
    if(depth=2,cat_id,three_cat_id),
    if(depth=2,cat_name,three_cat_name),
    if(depth=3,cat_id,four_cat_id),
    if(depth=3,cat_name,four_cat_name),
    is_leaf
 from(
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
    LEFT JOIN (select cat_id,cat_name,depth,parent_id from ods_fd_vb.ods_fd_category ) c_fou ON c_th.parent_id = c_fou.cat_id

)tab1;
