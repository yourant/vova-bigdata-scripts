insert overwrite table dim.dim_fd_category
SELECT
/*+ REPARTITION(1) */
c.cat_id,
c.cat_name,
c.depth,
case
    when c.depth = 1 then c.cat_id
    when c1.depth = 1 then c1.cat_id
    when c2.depth = 1 then c2.cat_id
    when c3.depth = 1 then c3.cat_id  end as fisrt_cat_id,
case
    when c.depth = 1 then c.cat_name
    when c1.depth = 1 then c1.cat_name
    when c2.depth = 1 then c2.cat_name
    when c3.depth = 1 then c3.cat_name  end as fisrt_cat_name,
case
    when c.depth = 2 then c.cat_id
    when c1.depth = 2 then c1.cat_id
    when c2.depth = 2 then c2.cat_id
    when c3.depth = 2 then c3.cat_id  end as second_cat_id,
case
    when c.depth = 2 then c.cat_name
    when c1.depth = 2 then c1.cat_name
    when c2.depth = 2 then c2.cat_name
    when c3.depth = 2 then c3.cat_name  end as second_cat_name,
case
    when c.depth = 3 then c.cat_id
    when c1.depth = 3 then c1.cat_id
    when c2.depth = 3 then c2.cat_id
    when c3.depth = 3 then c3.cat_id  end as third_cat_id,
case
    when c.depth = 3 then c.cat_name
    when c1.depth = 3 then c1.cat_name
    when c2.depth = 3 then c2.cat_name
    when c3.depth = 3 then c3.cat_name  end as third_cat_name,
case
    when c.depth > 0 THEN  1 ELSE 0 END AS is_leaf
FROM ods_fd_vb.ods_fd_category c
LEFT JOIN ods_fd_vb.ods_fd_category c1 ON c.parent_id = c1.cat_id
LEFT JOIN ods_fd_vb.ods_fd_category c2 ON c1.parent_id = c2.cat_id
LEFT JOIN ods_fd_vb.ods_fd_category c3 ON c2.parent_id = c3.cat_id;