
select
    bucket_floor,
    CONCAT(bucket_floor, ' to ', bucket_ceiling) as bucket_name,
    count(*) as count
from (
	select 
		floor(revenue/5.00)*5 as bucket_floor,
		floor(revenue/5.00)*5 + 5 as bucket_ceiling
	from web_sessions_table
) a
group by 1, 2
order by 1;
