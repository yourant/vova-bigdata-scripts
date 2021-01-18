INSERT overwrite table dim.dim_fd_language
select
    /*+ REPARTITION(1) */
    languages_id, 
    name, 
    code
from ods_fd_vb.ods_fd_languages;
