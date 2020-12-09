set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_region
select region_id, parent_id, region_name, region_type, region_cn_name, region_code
from ods_fd_ecshop.ods_fd_ecs_region_arc
where pt = '${pt}';
