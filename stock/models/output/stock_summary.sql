{{config(schema = 'analytics')}}

select symbol, date, open, high, low, close, volume, prev_close, change, gain, loss, avg_gain, avg_loss,
case when row_number() over(partition by symbol order by date) >= 15 then rsi else null end as rsi,
case when row_number() over(partition by symbol order by date) >= 20 then
avg(close) over(partition by symbol order by date rows between 19 preceding and current row) else null end as moving_average
from {{ref("stock_rolling_avg")}} order by symbol, date