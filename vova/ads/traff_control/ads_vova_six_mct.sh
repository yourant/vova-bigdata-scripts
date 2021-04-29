sql="
INSERT OVERWRITE TABLE ads.ads_vova_six_rank_mct_arc PARTITION (pt = '${pre_date}')
SELECT
/*+ REPARTITION(1) */
    m.mct_id,
    m.mct_name,
    g.first_cat_id,
    0 is_delete
FROM dim.dim_vova_merchant m
         JOIN dim.dim_vova_goods g ON m.mct_id = g.mct_id
WHERE m.mct_name = 'Bakers Store' AND first_cat_id = 194
   OR m.mct_name = 'Home\'s Store' AND first_cat_id = 5713
   OR m.mct_name = 'kak store' AND first_cat_id = 5777
   OR m.mct_name = 'lii' AND first_cat_id = 5712
   OR m.mct_name = 'Maup shop' AND first_cat_id = 5976
   OR m.mct_name = 'SBVCL STORE' AND first_cat_id = 5769
   OR m.mct_name = 'Story of Beauty' AND first_cat_id = 5715
   OR m.mct_name = 'yushu' AND first_cat_id = 5768
   OR m.mct_name = '17UM' AND first_cat_id = 5715
   OR m.mct_name = 'bajianzite' AND first_cat_id = 194
   OR m.mct_name = 'SHEINY' AND first_cat_id = 194
GROUP BY m.mct_id, m.mct_name, g.first_cat_id;
"