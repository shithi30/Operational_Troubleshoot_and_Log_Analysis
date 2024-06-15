/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=103365528
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

-- events
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, notification_id message_id, event_name, event_date schedule_date, bulk_notification_id request_id
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_date in('2022-02-27', '2022-03-03') -- 2 days in question
	and event_name in('inbox_message_received', 'inbox_message_open'); 

-- campaigns
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select message_id, request_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count, total_success    
from public.notification_bulknotificationsendrequest
where coalesce(date(schedule_time), date(updated_at)) in('2022-02-27', '2022-03-03'); 

-- summary
select 
	schedule_date,
	message_id,
	message, 
	intended_tg,
	tg_claimed_successful,
	merchants_received_message,
	merchants_opened_message
from 
	(select 
		schedule_date, 
		message_id, 
		count(distinct case when event_name='inbox_message_received' then mobile_no else null end) merchants_received_message, 
		count(distinct case when event_name='inbox_message_open' then mobile_no else null end) merchants_opened_message
	from 
		data_vajapora.help_b tbl1 
		left join 
		data_vajapora.help_a tbl2 using(message_id, schedule_date, request_id)
	group by 1, 2
	) tbl1 
	
	inner join 

	(select 
		schedule_date, 
		message_id, 
		sum(receiver_count) intended_tg, 
		sum(total_success) tg_claimed_successful
	from data_vajapora.help_b
	group by 1, 2
	) tbl2 using(message_id, schedule_date)
	
	inner join 
	
	(select id message_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message     
	from public.notification_pushmessage
	) tbl3 using(message_id)
order by 1, 2; 
	