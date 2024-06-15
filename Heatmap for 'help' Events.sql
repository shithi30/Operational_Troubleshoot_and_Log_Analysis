/*
- Viz: 
	- Numbers: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=779560651
	- Pct: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1798973451
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any):

How many users use HELP service (option in the app home screen). For example:
1. Visit Help after registrations
2. How many times one user visit Help in a month etc. 

Can you please share the user journey [ event ] correlation matrix as we used to share earlier.
>> This analysis could be interesting for new user base as this will give insight like - 
1. What % of new user want's help.

According to tendencies of the last 7 days' newly registered users: 
- ~14% users (530 users) visit 'help' on the day of registration. 
- They mostly visit 'help' from 'tally' page (37% of 'help' users). 
- After that, they visit it immediately after 'cashbox open' (11%) and 'app open' (9%) respectively. 
*/

drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select *, lead(event_name, 1) over(partition by event_date, mobile_no order by event_timestamp) next_event
from 
	(select mobile_no, event_name, event_date, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date>=current_date-7 and event_date<current_date
		and event_name not in('inbox_message_received', 'in_app_message_received')
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) event_date
	from public.register_usermobile 
	where date(created_at)>=current_date-7 and date(created_at)<current_date
	) tbl2 using(mobile_no, event_date); 

with 
 	temp_table as 
	(select 
		event_name, 
		next_event, 
		ceil(avg(merchants_moved)) avg_merchants_moved
	from 
		(select event_date, event_name, next_event, count(distinct mobile_no) merchants_moved
		from data_vajapora.help_c
		where next_event is not null
		group by 1, 2, 3
		) tbl1 
	group by 1, 2
	) 

select event_name, next_event, avg_merchants_moved, avg_merchants_moved/sum_merchants_moved avg_merchants_moved_pct
from 
	temp_table tbl1 
	
	inner join 
	
	(select next_event, sum(avg_merchants_moved) sum_merchants_moved
	from temp_table
	group by 1
	) tbl2 using(next_event); 

select 
	reg_date, 
	count(tbl1.mobile_no) reg_merchants, 
	count(distinct case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) reg_merchants_visited_help
from 
	(select date(created_at) reg_date, mobile_number mobile_no 
	from public.register_usermobile 
	where date(created_at)>=current_date-14 and date(created_at)<current_date
	) tbl1 
	
	left join 
		
	(select mobile_no, event_date reg_date
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date>=current_date-14 and event_date<current_date
		and event_name='help'
	) tbl2 using(mobile_no, reg_date)
group by 1 
order by 1; 
