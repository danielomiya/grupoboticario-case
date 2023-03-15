select
  brand_id,
  brand_name,
  product_line_id,
  product_line_name,
  sum(sale_amount) as amount_sold
from `staging.sales_base`
group by 1, 2, 3, 4
