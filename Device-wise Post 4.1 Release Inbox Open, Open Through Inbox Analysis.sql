/*
- Viz: 
- Data: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1540411461
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1006528933
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Need unregistered and unverified user data on daily basis
- Notes (if any): shared as .csvs
*/

-- for device-wise open through inbox (PUs)
do $$

declare 
	var_date date:='2022-02-18'::date; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select mobile_no, device_brand
	from 
		(select mobile mobile_no, max(id) id
		from public.registered_users 
		where 
			device_status='active'
			and date(created_at)<=var_date
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, case when device_brand in('xiaomi', 'Xiaomi') then 'xiaomi' else device_brand end device_brand
		from public.registered_users 
		) tbl2 using(id); 

	loop
		delete from data_vajapora.post_release_open_through_inbox_analysis_brand
		where report_date=var_date;
	
		insert into data_vajapora.post_release_open_through_inbox_analysis_brand		
		select
			var_date report_date, 
			device_brand,
			
			count(distinct case when event_name='app_opened' and tbl2.mobile_no is null then mobile_no else null end) below_41_app_opened_pus, 
			count(distinct case when event_name='open_through_inbox' and tbl2.mobile_no is null then mobile_no else null end) below_41_opened_through_inbox_pus, 
			count(distinct case when event_name='app_opened' and tbl2.mobile_no is not null then mobile_no else null end) in_41_app_opened_pus, 
			count(distinct case when event_name='open_through_inbox' and tbl2.mobile_no is not null then mobile_no else null end) in_41_opened_through_inbox_pus
		from 
			(select mobile_no, event_name
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('app_opened')
				
			union all 
			
			select mobile_no, 'open_through_inbox' event_name 
			from data_vajapora.mom_cjm_performance_detailed
			where report_date=var_date
			) tbl1 
			
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
from data_vajapora.post_release_open_through_inbox_analysis_brand; 

select 
	report_date, 
	
	sum(case when device_brand='samsung' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_samsung, 
	sum(case when device_brand='samsung' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_samsung, 
	
	sum(case when device_brand='vivo' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_vivo, 
	sum(case when device_brand='vivo' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_vivo,
	
	sum(case when device_brand='OPPO' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_OPPO, 
	sum(case when device_brand='OPPO' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_OPPO,
	
	sum(case when device_brand='xiaomi' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_xiaomi, 
	sum(case when device_brand='xiaomi' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_xiaomi,
	
	sum(case when device_brand='HUAWEI' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_HUAWEI, 
	sum(case when device_brand='HUAWEI' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_HUAWEI,
	
	sum(case when device_brand='Symphony' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_Symphony, 
	sum(case when device_brand='Symphony' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_Symphony,
	
	sum(case when device_brand='realme' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_realme, 
	sum(case when device_brand='realme' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_realme,
	
	sum(case when device_brand='Redmi' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_Redmi, 
	sum(case when device_brand='Redmi' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_Redmi,
	
	sum(case when device_brand='Itel' then below_41_opened_through_inbox_pus*1.00/below_41_app_opened_pus else 0 end) below_41_app_open_to_open_through_inbox_pus_ratio_Itel, 
	sum(case when device_brand='Itel' then in_41_opened_through_inbox_pus*1.00/in_41_app_opened_pus else 0 end) in_41_app_open_to_open_through_inbox_pus_ratio_Itel
from data_vajapora.post_release_open_through_inbox_analysis_brand
group by 1
order by 1; 

-- for device-wise inbox (PUs)
do $$

declare 
	var_date date:='2022-02-18'::date; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select mobile_no, device_brand
	from 
		(select mobile mobile_no, max(id) id
		from public.registered_users 
		where 
			device_status='active'
			and date(created_at)<=var_date
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, case when device_brand in('xiaomi', 'Xiaomi') then 'xiaomi' else device_brand end device_brand
		from public.registered_users 
		) tbl2 using(id); 

	loop
		delete from data_vajapora.post_release_inbox_analysis_brand
		where report_date=var_date;
	
		insert into data_vajapora.post_release_inbox_analysis_brand		
		select
			var_date report_date, 
			device_brand,
			
			count(distinct case when event_name='inbox_message_received' and tbl2.mobile_no is null then mobile_no else null end) below_41_inbox_received_users, 
			count(case when event_name='inbox_message_received' and tbl2.mobile_no is null then id else null end) below_41_inbox_received_events,
			count(distinct case when event_name='inbox_message_open' and tbl2.mobile_no is null then mobile_no else null end) below_41_inbox_open_users, 
			count(case when event_name='inbox_message_open' and tbl2.mobile_no is null then id else null end) below_41_inbox_open_events, 
			
			count(distinct case when event_name='inbox_message_received' and tbl2.mobile_no is not null then mobile_no else null end) in_41_inbox_received_users, 
			count(case when event_name='inbox_message_received' and tbl2.mobile_no is not null then id else null end) in_41_inbox_received_events,
			count(distinct case when event_name='inbox_message_open' and tbl2.mobile_no is not null then mobile_no else null end) in_41_inbox_open_users, 
			count(case when event_name='inbox_message_open' and tbl2.mobile_no is not null then id else null end) in_41_inbox_open_events
		from 
			(select mobile_no, id, event_name
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('inbox_message_received', 'inbox_message_open')
			) tbl1 
			
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
from data_vajapora.post_release_inbox_analysis_brand; 

select 
	report_date, 
	
	sum(case when device_brand='samsung' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_samsung, 
	sum(case when device_brand='samsung' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_samsung, 
	
	sum(case when device_brand='vivo' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_vivo, 
	sum(case when device_brand='vivo' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_vivo,
	
	sum(case when device_brand='OPPO' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_OPPO, 
	sum(case when device_brand='OPPO' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_OPPO,
	
	sum(case when device_brand='xiaomi' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_xiaomi, 
	sum(case when device_brand='xiaomi' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_xiaomi,
	
	sum(case when device_brand='HUAWEI' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_HUAWEI, 
	sum(case when device_brand='HUAWEI' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_HUAWEI,
	
	sum(case when device_brand='Symphony' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_Symphony, 
	sum(case when device_brand='Symphony' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_Symphony,
	
	sum(case when device_brand='realme' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_realme, 
	sum(case when device_brand='realme' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_realme,
	
	sum(case when device_brand='Redmi' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_Redmi, 
	sum(case when device_brand='Redmi' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_Redmi,
	
	sum(case when device_brand='Itel' then below_41_inbox_open_users*1.00/below_41_inbox_received_users else 0 end) below_41_inbox_receive_to_open_users_ratio_Itel, 
	sum(case when device_brand='Itel' then in_41_inbox_open_users*1.00/in_41_inbox_received_users else 0 end) in_41_inbox_receive_to_open_users_ratio_Itel
from data_vajapora.post_release_inbox_analysis_brand
group by 1
order by 1; 
