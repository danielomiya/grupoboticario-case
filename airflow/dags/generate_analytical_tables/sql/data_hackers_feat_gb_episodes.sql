select
  id, name, description, release_date, duration_ms, language, explicit, type
from `staging.data_hackers_episodes`
where lower(name) like '%grupo botic%' or lower(description) like '%grupo botic%'
