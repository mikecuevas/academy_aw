with total_sales as (
  select
    sum(extended_amount) as total_sales_2011
  from {{ ref('fct_vendas') }}
  where orderdate between '2011-01-01' and '2011-12-31'
)

select
  case
    when abs(total_sales_2011 - 12646112.16) < 0.001 then 'PASS' else 'FAIL'
  end as test_result
from total_sales
