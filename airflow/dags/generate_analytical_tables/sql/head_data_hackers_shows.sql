select
  id, name, description, total_episodes
from `staging.data_hackers_shows`
where name = 'Data Hackers'
limit 50
