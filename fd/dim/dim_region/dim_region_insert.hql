INSERT overwrite table dim.dim_fd_region
select /*+ REPARTITION(1) */ s.region_id,
       if(s.region_code is null or s.region_code = '', s.region_name_en, s.region_code) as region_code,
       s.region_name_en,
       s.region_name_cn,
       s.first_region_id,
       if(s.first_region_code is null or s.first_region_code = '', s.first_region_name_en,s.first_region_code) as first_region_code,
       s.first_region_name_en,
       s.first_region_name_cn,
       s.second_region_id,
       if(s.second_region_code is null or s.second_region_code = '', s.second_region_name_en,s.second_region_code)  as second_region_code,
       s.second_region_name_en,
       s.second_region_name_cn,
       s.area_id,
       t.area_name,
       t.area_name_cn,
       t.continent_id,
       t.continent_name,
       t.continent_name_cn
from (
         SELECT t1.region_id,
                t1.region_code,
                t1.region_name                                                          AS region_name_en,
                t1.chinese_region_name                                                  AS region_name_cn,
                t1.area_id,
                case
                    when t1.parent_id = 0 then ''
                    when t2.parent_id = 0 then t2.region_id
                    when t3.parent_id = 0 then t3.region_id
                    end                                                                 AS first_region_id,
                case
                    when t1.parent_id = 0 then ''
                    when t2.parent_id = 0 then t2.region_code
                    when t3.parent_id = 0 then t3.region_code
                    end                                                                 AS first_region_code,
                case
                    when t1.parent_id = 0 then ''
                    when t2.parent_id = 0 then t2.region_name
                    when t3.parent_id = 0 then t3.region_name
                    end                                                                 AS first_region_name_en,
                case
                    when t1.parent_id = 0 then ''
                    when t2.parent_id = 0 then t2.chinese_region_name
                    when t3.parent_id = 0 then t3.chinese_region_name
                    end                                                                 AS first_region_name_cn,
                IF(t1.parent_id <> 0 and t2.parent_id <> 0, t2.region_id, '')           AS second_region_id,
                IF(t1.parent_id <> 0 and t2.parent_id <> 0, t2.region_code, '')         AS second_region_code,
                IF(t1.parent_id <> 0 and t2.parent_id <> 0, t2.region_name, '')         AS second_region_name_en,
                IF(t1.parent_id <> 0 and t2.parent_id <> 0, t2.chinese_region_name, '') AS second_region_name_cn

         FROM (
                  SELECT region_id, region_code, region_name, chinese_region_name, parent_id, area_id
                  FROM ods_fd_vb.ods_fd_region
              ) t1
                  left join
              (
                  SELECT region_id, region_code, region_name, chinese_region_name, parent_id, area_id
                  FROM ods_fd_vb.ods_fd_region
              ) t2 on t1.parent_id = t2.region_id
                  left join
              (
                  SELECT region_id, region_code, region_name, chinese_region_name, parent_id, area_id
                  FROM ods_fd_vb.ods_fd_region
              ) t3 on t2.parent_id = t3.region_id
     ) s
         left join
     (
         SELECT t1.id                                            as area_id,
                t1.region_area_name                              as area_name,
                t1.region_area_cn_name                           as area_name_cn,
                if(t1.parent_id = 0, '', t2.id)                  AS continent_id,
                if(t1.parent_id = 0, '', t2.region_area_name)    AS continent_name,
                if(t1.parent_id = 0, '', t2.region_area_cn_name) AS continent_name_cn
         FROM (
                  SELECT id,
                         region_area_name,
                         region_area_cn_name,
                         parent_id
                  FROM ods_fd_vb.ods_fd_region_area
              ) t1
                  left join
              (
                  SELECT id, region_area_name, region_area_cn_name
                  FROM ods_fd_vb.ods_fd_region_area
              ) t2
              ON t1.parent_id = t2.id
     ) t on s.area_id = t.area_id;
