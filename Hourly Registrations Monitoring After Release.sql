/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1072443165
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
*/

-- in live
select 
	date(created_at) reg_date, 
	date_part('hour', created_at) reg_hour, 
	count(mobile_number) reg_merchants 
from public.register_usermobile 
where date(created_at)>=current_date-21 and date(created_at)<=current_date
group by 1, 2
order by 1, 2; 