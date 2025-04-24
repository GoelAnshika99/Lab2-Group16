{{config(schema = 'staging')}}

select *,
avg(gain) over(partition by symbol order by date rows between 13 preceding and current row) as avg_gain,
avg(loss) over(partition by symbol order by date rows between 13 preceding and current row) as avg_loss,
case when avg(loss) over(partition by symbol order by date rows between 13 preceding and current row) = 0 then 100
else 100 - (100/(1+(avg(gain) over(partition by symbol order by date rows between 13 preceding and current row)/avg(loss) over(partition by symbol order by date rows between 13 preceding and current row)))) end as rsi
from {{source('staging', 'stock_price_diff')}}