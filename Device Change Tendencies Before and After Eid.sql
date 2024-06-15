/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=921402601
- Data: 
- Function: 
- Table:
- Instructions: Data team, kindly help us to know how many users are stuck in inactive devices due to device change.
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	I have analysed device changing behaviour before and after Eid closures within 14-day timeframes. 
	- Before Eid, an avg. 30 merchants would change devices daily
	- After Eid, an. avg. 23 merchants are changing devices daily 
	There could be 2 things going on here: 
	- Either merchants have reduced buying new phones (due to inflations etc.)
	- Or, devices are being changed but not being recorded (less likely)
*/

select
	date(next_created_at) device_change_date, 
	count(distinct mobile) device_changed_merchants
from 
	(select 
		mobile, 
		device_id, 
		device_status, 
		created_at, 
		lead(device_id, 1) over(partition by mobile order by created_at asc) next_device_id,
		lead(device_status, 1) over(partition by mobile order by created_at asc) next_device_status,
		lead(created_at, 1) over(partition by mobile order by created_at asc) next_created_at
	from public.register_historicalregistereduser
	where date(created_at)>current_date-90
	) tbl1 
where 
	device_id!=next_device_id 
	and device_status='inactive'
	and next_device_status='active'
group by 1 
order by 1; 
