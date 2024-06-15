/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=677474927
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1246570278
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

-- for OS-wise in_app (PUs)
do $$

declare 
	var_date date:='2022-02-18'::date; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select mobile_no, os_version
	from 
		(select mobile mobile_no, max(id) id
		from public.registered_users 
		where 
			device_status='active'
			and date(created_at)<=var_date
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, os_version
		from public.registered_users 
		) tbl2 using(id);

	loop
		delete from data_vajapora.post_release_in_app_analysis_os
		where report_date=var_date;
	
		insert into data_vajapora.post_release_in_app_analysis_os		
		select
			var_date report_date, 
			os_version,
			
			count(distinct case when event_name='in_app_message_received' and tbl2.mobile_no is null then mobile_no else null end) below_41_in_app_received_users, 
			count(case when event_name='in_app_message_received' and tbl2.mobile_no is null then id else null end) below_41_in_app_received_events,
			count(distinct case when event_name='in_app_message_open' and tbl2.mobile_no is null then mobile_no else null end) below_41_in_app_open_users, 
			count(case when event_name='in_app_message_open' and tbl2.mobile_no is null then id else null end) below_41_in_app_open_events, 
			
			count(distinct case when event_name='in_app_message_received' and tbl2.mobile_no is not null then mobile_no else null end) in_41_in_app_received_users, 
			count(case when event_name='in_app_message_received' and tbl2.mobile_no is not null then id else null end) in_41_in_app_received_events,
			count(distinct case when event_name='in_app_message_open' and tbl2.mobile_no is not null then mobile_no else null end) in_41_in_app_open_users, 
			count(case when event_name='in_app_message_open' and tbl2.mobile_no is not null then id else null end) in_41_in_app_open_events
		from 
			(select mobile_no, id, event_name, bulk_notification_id
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('in_app_message_received', 'in_app_message_open')
			) tbl1 
			
			inner join 
			
			(select distinct request_id bulk_notification_id
			from 
				(select message_id, request_id, coalesce(date(schedule_time), date(updated_at)) schedule_date   
				from public.notification_bulknotificationsendrequest
				) tbl1 
				
				inner join 
				
				(select id message_id
				from public.notification_pushmessage
				where "type" ='POPUP_MESSAGE'
				) tbl2 using(message_id)
			where schedule_date=var_date
			) tbl5 using(bulk_notification_id)
			
			inner join 
			
			(select distinct mobile_no
			from tallykhata.tk_power_users_10
			where report_date=var_date
			) tbl4 using(mobile_no)
			
			left join 
				
			(select mobile_no
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date
				and app_version='4.1'
			) tbl2 using(mobile_no)
			
			left join 
			
			data_vajapora.help_a tbl3 using(mobile_no)
		group by 1, 2; 	

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.post_release_in_app_analysis_os; 

select 
	report_date, 
	
	sum(case when os_version='10' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_10", 
	sum(case when os_version='10' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_10", 
	
	sum(case when os_version='9' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_9", 
	sum(case when os_version='9' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_9",
	
	sum(case when os_version='11' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_11", 
	sum(case when os_version='11' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_11",
	
	sum(case when os_version='8.1.0' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_8.1.0", 
	sum(case when os_version='8.1.0' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_8.1.0",
	
	sum(case when os_version='6.0' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_6.0", 
	sum(case when os_version='6.0' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_6.0",
	
	sum(case when os_version='7.0' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_7.0", 
	sum(case when os_version='7.0' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_7.0",
	
	sum(case when os_version='6.0.1' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_6.0.1", 
	sum(case when os_version='6.0.1' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_6.0.1",
	
	sum(case when os_version='5.1.1' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_5.1.1", 
	sum(case when os_version='5.1.1' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_5.1.1",
	
	sum(case when os_version='8.0.0' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_8.0.0", 
	sum(case when os_version='8.0.0' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_8.0.0", 
	
	sum(case when os_version='7.1.1' then below_41_in_app_open_users*1.00/below_41_in_app_received_users else 0 end) "below_41_in_app_receive_to_open_users_ratio_7.1.1", 
	sum(case when os_version='7.1.1' then in_41_in_app_open_users*1.00/in_41_in_app_received_users else 0 end) "in_41_in_app_receive_to_open_users_ratio_7.1.1"
from data_vajapora.post_release_in_app_analysis_os
where 
	in_41_in_app_received_users!=0
	and below_41_in_app_received_users!=0
group by 1
order by 1; 

-- for device-wise in_app
do $$

declare 
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select mobile_no, os_version
	from 
		(select mobile mobile_no, max(id) id
		from public.registered_users 
		where 
			device_status='active'
			and date(created_at)<=var_date
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, os_version
		from public.registered_users 
		) tbl2 using(id);

	loop
		delete from data_vajapora.post_release_in_app_analysis_os_2
		where report_date=var_date;
	
		insert into data_vajapora.post_release_in_app_analysis_os_2
		select
			var_date report_date, 
			os_version,
			
			count(distinct case when event_name='in_app_message_received' then mobile_no else null end) in_app_received_users, 
			count(case when event_name='in_app_message_received' then id else null end) in_app_received_events,
			count(distinct case when event_name='in_app_message_open' then mobile_no else null end) in_app_open_users, 
			count(case when event_name='in_app_message_open' then id else null end) in_app_open_events
		from 
			(select mobile_no, id, event_name, notification_id message_id, bulk_notification_id 
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('in_app_message_received', 'in_app_message_open')
			) tbl1 
			
			inner join 
			
			(select distinct request_id bulk_notification_id
			from 
				(select message_id, request_id, coalesce(date(schedule_time), date(updated_at)) schedule_date   
				from public.notification_bulknotificationsendrequest
				) tbl1 
				
				inner join 
				
				(select id message_id
				from public.notification_pushmessage
				where "type" ='POPUP_MESSAGE'
				) tbl2 using(message_id)
			where schedule_date=var_date
			) tbl2 using(bulk_notification_id)
			
			left join 
			
			data_vajapora.help_a tbl3 using(mobile_no)
		group by 1, 2; 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.post_release_in_app_analysis_os_2; 

select 
	report_date, 
	sum(case when os_version='10' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_10", 
	sum(case when os_version='9' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_9", 
	sum(case when os_version='11' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_11", 
	sum(case when os_version='8.1.0' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_8.1.0", 
	sum(case when os_version='6.0' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_6", 
	sum(case when os_version='7.0' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_7", 
	sum(case when os_version='6.0.1' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_6.0.1", 
	sum(case when os_version='5.1.1' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_5.1.1", 
	sum(case when os_version='8.0.0' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_8.0.0", 
	sum(case when os_version='7.1.1' then  in_app_open_users*1.00/ in_app_received_users else 0 end)  "in_app_receive_to_open_users_ratio_7.1.1"
from data_vajapora.post_release_in_app_analysis_os_2
group by 1
order by 1; 
