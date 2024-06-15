/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1270982623
- Data: 
- Function: 
- Table:
- Instructions: https://docs.google.com/spreadsheets/d/1WrqQRJ38f8J-WTDK3LsvaCXOZVi5mrPtmj8KUqv_kxI/edit#gid=0
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Tables to have updated: 
	- public.notification_bulknotificationrequest
	- public.notification_bulknotificationsendrequest
	- public.notification_bulknotificationschedule
	- public.notification_pushmessage
	- public.register_tag
	
	Use script in 'Path' to update. 
	
	Match counts in live and DWH: 
	select count(*) from public.notification_bulknotificationrequest; 
	select count(*) from public.notification_bulknotificationsendrequest; 
	select count(*) from public.notification_bulknotificationschedule; 
	select count(*) from public.notification_pushmessage; 
	select count(*) from public.register_tag;
	
	Investigate for a single case:
	select * from public.notification_bulknotificationrequest where id=12511; 					-- create
	select * from public.notification_bulknotificationsendrequest where request_id=12511; 		-- schedule
	select * from public.notification_bulknotificationschedule where request_id=12511; 			-- periodic schedule
*/

do $$

declare 
	var_date date:=current_date-5; 

begin  
	raise notice 'New OP goes below:'; 	

	-- created+scheduled
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select
		created_at::date create_date, 
		concat(schedule_date::text, ', ', schedule_dates) schedule_dates, coalesce(tbl3.schedule_time, tbl2.schedule_time) schedule_time, 
		campaign_id, created_by, bulk_notification_id, message_id,
		intended_receiver_count, total_success, 
		coalesce(tbl2.status, tbl1.status) status, 
		schedule_id, schedule_type, should_repeat, repeat_days, repeat_until,
		message, msg_type, 
		tag_id , tag_name, tag_description
	from   
		(select 
			title campaign_id, message_id, id bulk_notification_id, created_at, created_by, receiving_tag_id tag_id, 
			case 
				when status=1 then 'processing'
				when status=2 then 'ready' 
			end status
		from public.notification_bulknotificationrequest
		) tbl1
		
		left join 
		    
		(select 
			request_id bulk_notification_id, 
			coalesce(schedule_time::time, updated_at::time) schedule_time, 
			coalesce(schedule_time::date, updated_at::date) schedule_date, 
			receiver_count intended_receiver_count, total_success, 
			case 
				when status=1 then 'scheduled'
				when status=2 then 'processing' 
				when status=3 then 'complete' 
				when status=4 then 'canceled' 
				when status=5 then 'in progress' 
			end status
		from public.notification_bulknotificationsendrequest
		) tbl2 using(bulk_notification_id)
		
		left join 
		
		(select 
			request_id bulk_notification_id, id schedule_id, schedule_type, schedule_time, 
			should_repeat, repeat_days, repeat_until,
			(select string_agg(series_dates::text, ', ') limited_dates 
			from 
				(select generate_series(0, repeat_until::date-created_at::date, 1)+created_at::date series_dates
				) tbl1
			where right(series_dates::text, 2) in 
				(select case when length(specified_dates)=1 then concat('0', specified_dates) else specified_dates end specified_dates 
				from 
					(select unnest(string_to_array(trim(translate(repeat_days, '[]', '  ')), ', ')) specified_dates
					) tbl1
				)
			) schedule_dates
		from public.notification_bulknotificationschedule 
		) tbl3 using(bulk_notification_id)
		
		left join 
	
		(select 
			id tag_id, 
			tag_name, 
			case 
				when tag_description is not null then tag_description
				when tag_name='NBAll' then 'NB0_NN1_NN2-6'
				when tag_name='LTUAll' then 'LTUCb_LTUTa'
				when tag_name='3RAUAll' then '3RAUCb_3RAU Set-A_3RAU Set-B_3RAU Set-C_3RAUTa_3RAUTa+Cb_3RAUTacs'
				when tag_name='PUAll' then 'PUCb_PU Set-A_PU Set-B_PU Set-C_PUTa_PUTa+Cb_PUTacs'
				when tag_name='ZAll' then 'ZCb_ZTa_ZTa+Cb'
				when tag_name in(select distinct tg from cjm_segmentation.retained_users where report_date=current_date-1) then tag_name 
				else null
			end tag_description
		from public.register_tag
		) tbl5 using(tag_id)
	
		inner join 
		
		(select 
			id message_id, 
			case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message, 
			"type" msg_type
		from public.notification_pushmessage
		) tbl4 using(message_id);
	
	loop
		delete from data_vajapora.delivery_issue_inv
		where schedule_date=var_date; 
	
		-- DB footprint on date
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select bulk_notification_id, event_name, mobile_no, id
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date>=var_date -- and event_date<var_date+5
			and event_name in('in_app_message_received', 'in_app_message_open', 'inbox_message_received', 'inbox_message_open')
			and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a where schedule_dates like concat('%', var_date::text, '%')); 
		
		-- metrics
		insert into data_vajapora.delivery_issue_inv
		select 
			create_date,
			var_date schedule_date,
			campaign_id,
			bulk_notification_id,
			message_id,
			(case 
				when intended_receiver_count is not null then intended_receiver_count
				when tag_description is null then null
				else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=create_date and tg in(select * from unnest(string_to_array(tag_description, '_'))))
			end 
			) intended_receiver_count,        
			total_success,
			schedule_id,
			schedule_type,
			schedule_time,
			should_repeat,
			repeat_days,
			repeat_until,
			
			in_app_received_users,
			in_app_open_users,
			in_app_received_events, 
			in_app_open_events,
			inbox_received_users,
			inbox_open_users,
			inbox_received_events, 
			inbox_open_events, 
			
			message,
			msg_type, 
			
			-- added for enhanced reporting
			created_by, tag_id , tag_name, tag_description
		from 
			(select * 
			from data_vajapora.help_a 
			where schedule_dates like concat('%', var_date::text, '%')
			) tbl1 
			
			left join 
						
			(select 
				bulk_notification_id, 
				
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) in_app_received_users, 
				count(case when event_name='in_app_message_received' then id else null end) in_app_received_events,
				count(distinct case when event_name='in_app_message_open' then mobile_no else null end) in_app_open_users, 
				count(case when event_name='in_app_message_open' then id else null end) in_app_open_events,
				
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) inbox_received_users, 
				count(case when event_name='inbox_message_received' then id else null end) inbox_received_events,
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) inbox_open_users, 
				count(case when event_name='inbox_message_open' then id else null end) inbox_open_events
			from data_vajapora.help_b
			group by 1
			) tbl2 using(bulk_notification_id); 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	create_date,
	created_by, 
	schedule_date,
	campaign_id,
	bulk_notification_id,
	intended_receiver_count,
	total_success claimed_success, 
	
	schedule_type,
	
	in_app_received_users,
	in_app_open_users,
	in_app_received_events,
	in_app_open_events,
	inbox_received_users,
	inbox_open_users,
	inbox_received_events,
	inbox_open_events,
	
	message_id,
	message,
	msg_type, 
	
	case 
		when in_app_received_users is null and inbox_received_users is null and intended_receiver_count=0 and total_success=0 then 'TG size found 0'
		when in_app_received_users is null and inbox_received_users is null and intended_receiver_count>0 and total_success=0 then 'Firebase shows 0 success'
		when in_app_received_users is null and inbox_received_users is null and intended_receiver_count>0 and total_success>0 then 'Firebase success but no events'
		when in_app_received_users is null and inbox_received_users is null and intended_receiver_count is null and total_success is null then 'scheduled repetitively but failed'
		else null
	end failure_category_if_failed
from data_vajapora.delivery_issue_inv
where
	campaign_id like '%-%'
	and in_app_received_users is null and inbox_received_users is null
	and schedule_date<current_date; 

select failure_category_if_failed, count(*) campaigns, min(schedule_date) first_occured, count(distinct message_id) message_missed
from data_vajapora.help_b
group by 1; 

select * 
from data_vajapora.help_b
order by 3; 
