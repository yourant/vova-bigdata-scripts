INSERT OVERWRITE TABLE dim.dim_fd_goods_finder
SELECT
      /*+ REPARTITION(1) */
       goods_id,
       virtual_goods_id,
       lower(collect_set(project_name)[0]),
       REGEXP_REPLACE(collect_set(finder)[0], '[^0-9a-zA-Z,.]', '')
FROM (
         select gf.goods_id,
                gf.virtual_goods_id,
                gf.project_name,
                gf.finder
         from (
                  select gp.goods_id,
                         cast(gp.virtual_goods_id as bigint) as virtual_goods_id,
                         p.name as project_name,
                         gp.goods_selector                   as finder
                  from ods_fd_dmc.ods_fd_dmc_goods_project gp
                           left join ods_fd_fam.ods_fd_fam_party p on p.party_id = gp.party_id
              ) gf
         where gf.finder is not null
           and gf.finder != ''
           and gf.finder != '0'
           and gf.virtual_goods_id != 0
           and gf.goods_id != 0
     )
group by goods_id, virtual_goods_id;