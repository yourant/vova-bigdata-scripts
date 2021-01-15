INSERT overwrite table dim.dim_fd_language
select 
    languages_id, 
    name, 
    code
from ods_fd_vb.ods_fd_languages;
