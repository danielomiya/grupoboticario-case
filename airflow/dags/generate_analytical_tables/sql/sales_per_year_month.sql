select
  extract(year from sale_date) as year,
  extract(month from sale_date) as month,
  sum(sale_amount) as amount_sold
from `staging.sales_base`
group by 1, 2
order by 1 desc, 2 desc
