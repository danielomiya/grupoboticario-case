select
  product_line_id,
  product_line_name,
  extract(year from sale_date) as year,
  extract(month from sale_date) as month,
  sum(sale_amount) as amount_sold
from `staging.sales_base`
group by 1, 2, 3, 4
