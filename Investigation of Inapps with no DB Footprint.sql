/*
- Viz: 
- Data: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=113618669
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1790670905
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

-- inapps with no DB footprint
do $$

declare 
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.daily_bulk_inapp_inv
		where schedule_date=var_date;
	
		-- bulk inapp campaigns scheduled on date
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select *
		from 
			(select message_id, request_id bulk_notification_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count intended_receiver_count, total_success
			from public.notification_bulknotificationsendrequest
			) tbl1 
			
			inner join 
			
			(select id message_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message 
			from public.notification_pushmessage
			where "type" ='POPUP_MESSAGE'
			) tbl2 using(message_id)
		where schedule_date=var_date; 
			
		-- bulk inapp campaigns' DB footprint
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select bulk_notification_id, event_name, mobile_no, id
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date>=var_date
			and event_name in('in_app_message_received', 'in_app_message_open')
			and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_b); 
	
		-- summary metrics
		insert into data_vajapora.daily_bulk_inapp_inv
		select 
			schedule_date,
			bulk_notification_id,
			message_id,
			message,
			coalesce(intended_receiver_count, 0) intended_tg,
			total_success claimed_success, 
			coalesce(in_app_received_users, 0) in_app_received_users,
			coalesce(in_app_open_users, 0) in_app_open_users,
			coalesce(in_app_received_events, 0) in_app_received_events, 
			coalesce(in_app_open_events, 0) in_app_open_events
		from 
			data_vajapora.help_b tbl1 
			
			left join 
						
			(select 
				bulk_notification_id, 
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) in_app_received_users, 
				count(case when event_name='in_app_message_received' then id else null end) in_app_received_events,
				count(distinct case when event_name='in_app_message_open' then mobile_no else null end) in_app_open_users, 
				count(case when event_name='in_app_message_open' then id else null end) in_app_open_events
			from data_vajapora.help_a
			group by 1
			) tbl2 using(bulk_notification_id); 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	schedule_date,
	campaign_id, 
	bulk_notification_id,
	message_id,
	message,
	intended_tg,
	claimed_success,
	in_app_received_users,
	in_app_open_users,
	in_app_received_events,
	in_app_open_events,
	case when message_id in(2786, 2787) then 'forced video campaign' else 'normal inapp campaign' end campaign_type, 
	in_app_open_users*1.00/in_app_received_users received_to_open_users_pct
from 
	data_vajapora.daily_bulk_inapp_inv tbl1 
	
	left join 
	
	(select id bulk_notification_id, title campaign_id
    from public.notification_bulknotificationrequest
    ) tbl2 using(bulk_notification_id); 

-- investigation
select 
	*, 
	case when message_id in(2786, 2787) then 'forced video campaign' else 'normal inapp campaign' end campaign_type, 
	in_app_open_users*1.00/in_app_received_users received_to_open_users_pct
from 
	data_vajapora.daily_bulk_inapp_inv tbl1 
	
	left join 
	
	(select id bulk_notification_id, title campaign_id
    from public.notification_bulknotificationrequest
    ) tbl2 using(bulk_notification_id) 
where 
	campaign_id in(
		-- 08-Feb
		'RC220208-25',
		'RC220208-26',
		'RC220208-27',
		'RC220208-28',
		'RC220208-29',
		'RC220208-30',
		'RC220208-31',
	
		-- 09-Feb
		'RC220209-25',
		'RC220209-26',
		'RC220209-27',
		'RC220209-28',
		'RC220209-29',
		'RC220209-30',
		'RC220209-31',
	
		-- 10-Feb
		'RC220210-17',
		'RC220210-18',
		'RC220210-19',
		'RC220210-20',
		'RC220210-21',
		'RC220210-22',
		'RC220210-23', 

		-- 11-Feb
		'RC220211-24',
		'RC220211-25',
		'RC220211-26',
		'RC220211-27',
		'RC220211-28',
		'RC220211-29',
		'RC220211-30',
		'RC220211-31', 
		
		-- 12-Feb
		'RC220212-25',
		'RC220212-26',
		'RC220212-27',
		'RC220212-28',
		'RC220212-29',
		'RC220212-30',
		'RC220212-31', 
		
		-- 13-Feb
		'RC220213-25',
		'RC220213-26',
		'RC220213-27',
		'RC220213-28',
		'RC220213-29',
		'RC220213-30',
		'RC220213-31'
	)
order by 
	schedule_date asc,
	in_app_open_users*1.00/in_app_received_users asc; 

