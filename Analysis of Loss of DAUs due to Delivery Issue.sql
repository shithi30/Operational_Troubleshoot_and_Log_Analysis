/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=419454848
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
	Last two days DAU is around 245k.
	Seems existing users are visiting less.
	Any impact of less delivery of messaging? @Samir bh
	
	Yes, it does seem to have an impact. 
	DAU through inbox messaging is 25% lower for the last 2 days in comparison with last week. This accounts for ~2500 less DAUs. 
	Also, inbox opens have declined by 20%, accounting for 12k less users visiting inbox. 
*/

select 
	report_date, 
	dau, 
	inbox_opened_merchants,  
	open_through_inbox_merchants,   
	first_open_through_inbox_merchants, 
	first_open_through_inbox_merchants*1.00/dau first_open_through_inbox_merchants_pct
from 
	data_vajapora.personalized_msg_impact_1 tbl1
	
	inner join
		
	(-- dashboard DAU
	select 
		tbl_1.report_date,
		tbl_1.total_active_user_db_event dau
	from 
		(
		select 
			d.report_date,
			'T + Event [ DB ]' as category,
			sum(d.total_active_user) as total_active_user_db_event
		from tallykhata.tallykhata.daily_active_user_data as d 
		where d.category in('db_plus_event_date','Non Verified')
		group by d.report_date
		) as tbl_1 
	) tbl2 using(report_date)
	
	inner join 
	
	(-- TG info
	select 
		schedule_date report_date,
		sum(intended_receiver_count) intended_inbox_send_events, 
		sum(claimed_total_success) inbox_send_events_claimed_successful, 
		max(intended_receiver_count) intended_inbox_tg, 
		max(claimed_total_success) inbox_tg_claimed_successful
	from 
		(select 
			message_id, 
			schedule_date, 
			sum(receiver_count) intended_receiver_count, 
			sum(total_success) claimed_total_success
		from 
			(select message_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count, total_success    
			from public.notification_bulknotificationsendrequest
			) tbl1 
			
			inner join 
			
			(select id message_id
			from public.notification_pushmessage
			) tbl2 using(message_id)
		group by 1, 2
		) tbl1
	group by 1
	) tbl3 using(report_date)
order by 1; 
