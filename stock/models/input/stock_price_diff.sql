{{config(schema = 'staging')}}

select *, lag(close) over(partition by symbol order by date) as prev_close, close - lag(close) over(partition by symbol order by date) as change,
case when close - lag(close) over(partition by symbol order by date) > 0 then close - lag(close) over(partition by symbol order by date) else 0 end as gain,
case when close - lag(close) over(partition by symbol order by date) < 0 then abs(close - lag(close) over(partition by symbol order by date)) else 0 end as loss
from {{source('raw', 'stock_price')}}