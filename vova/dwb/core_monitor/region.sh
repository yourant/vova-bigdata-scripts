echo "
国家分组维表 新增维度再修改
insert overwrite table tmp.tmp_vova_core_monitor_region_group 
select
/*+ REPARTITION(1) */
region_id,
region_code,
region_group
from
(
select region_id,region_code,region_code region_group from ods_vova_vts.ods_vova_region where region_code in ('FR','IT','DE','ES','GB','CZ','CH','BE','RE','PL') and parent_id=0 and region_id!=8802
union all
select region_id,region_code,'TOP10' region_group from ods_vova_vts.ods_vova_region where region_code in ('FR','IT','DE','ES','GB','CZ','CH','BE','RE','PL') and parent_id=0 and region_id!=8802
union all
select region_id,region_code,'TOP20' region_group from ods_vova_vts.ods_vova_region where region_code in ('FR','IT','DE','ES','GB','CZ','CH','BE','RE','PL','AT','US','SK','AU','IL','DK','GP','BR','NO','PE') and parent_id=0 and region_id!=8802
union all
select region_id,region_code,'TOP10_EU_8' region_group from ods_vova_vts.ods_vova_region where region_code in ('IT','DE','ES','GB','CZ','CH','BE','PL') and parent_id=0 and region_id!=8802
union all
select region_id,region_code,'TOP20_EU_12' region_group from ods_vova_vts.ods_vova_region where region_code in ('IT','DE','ES','GB','CZ','CH','BE','PL','AT','SK','DK','NO') and parent_id=0 and region_id!=8802
) t;
"