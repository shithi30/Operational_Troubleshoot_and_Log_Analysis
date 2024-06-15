/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1381024387
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=263460516
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1540411461
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

We have compared inapp/inbox messages' receive-to-view ratios between 4.1 and lower version users.
- A clear ~5% improvement is observed in inbox performance. 
- No clear tendency could be found for inapp (neither overall nor devicve-wise).  

Data, Viz: 
- overall (inapp+inbox): https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1381024387
- device-wise (only inapp): https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=263460516

*/

-- for inapp
do $$

declare 
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.post_release_inapp_analysis
		where report_date=var_date;
	
		insert into data_vajapora.post_release_inapp_analysis		
		select
			var_date report_date, 
			
			count(distinct case when event_name='in_app_message_received' and tbl2.mobile_no is null then mobile_no else null end) below_41_inapp_received_users, 
			count(case when event_name='in_app_message_received' and tbl2.mobile_no is null then id else null end) below_41_inapp_received_events,
			count(distinct case when event_name='in_app_message_open' and tbl2.mobile_no is null then mobile_no else null end) below_41_inapp_open_users, 
			count(case when event_name='in_app_message_open' and tbl2.mobile_no is null then id else null end) below_41_inapp_open_events, 
			
			count(distinct case when event_name='in_app_message_received' and tbl2.mobile_no is not null then mobile_no else null end) in_41_inapp_received_users, 
			count(case when event_name='in_app_message_received' and tbl2.mobile_no is not null then id else null end) in_41_inapp_received_events,
			count(distinct case when event_name='in_app_message_open' and tbl2.mobile_no is not null then mobile_no else null end) in_41_inapp_open_users, 
			count(case when event_name='in_app_message_open' and tbl2.mobile_no is not null then id else null end) in_41_inapp_open_events
		from 
			(select mobile_no, id, event_name
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('in_app_message_received', 'in_app_message_open')
			) tbl1 
			
			left join 
				
			(select mobile_no
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date
				and app_version='4.1'
			) tbl2 using(mobile_no); 
				
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.post_release_inapp_analysis; 

-- for inbox
do $$

declare 
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.post_release_inbox_analysis
		where report_date=var_date;
	
		insert into data_vajapora.post_release_inbox_analysis		
		select
			var_date report_date, 
			
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
			
			left join 
				
			(select mobile_no
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date
				and app_version='4.1'
			) tbl2 using(mobile_no); 
				
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.post_release_inbox_analysis; 

select *
from 
	(select 
		*, 
		below_41_inapp_open_users*1.00/below_41_inapp_received_users below_41_inapp_receive_to_open_users_ratio, 
		below_41_inapp_open_events*1.00/below_41_inapp_received_events below_41_inapp_receive_to_open_events_ratio,
		in_41_inapp_open_users*1.00/in_41_inapp_received_users in_41_inapp_receive_to_open_users_ratio, 
		in_41_inapp_open_events*1.00/in_41_inapp_received_events in_41_inapp_receive_to_open_events_ratio
	from data_vajapora.post_release_inapp_analysis
	where report_date>'2022-02-17'
	) tbl1 
	
	inner join 
	
	(select 
		*, 
		below_41_inbox_open_users*1.00/below_41_inbox_received_users below_41_inbox_receive_to_open_users_ratio, 
		below_41_inbox_open_events*1.00/below_41_inbox_received_events below_41_inbox_receive_to_open_events_ratio,
		in_41_inbox_open_users*1.00/in_41_inbox_received_users in_41_inbox_receive_to_open_users_ratio, 
		in_41_inbox_open_events*1.00/in_41_inbox_received_events in_41_inbox_receive_to_open_events_ratio
	from data_vajapora.post_release_inbox_analysis
	where report_date>'2022-02-17'
	) tbl2 using(report_date); 

-- for device-wise inapp
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
		delete from data_vajapora.post_release_inapp_analysis_brand
		where report_date=var_date;
	
		insert into data_vajapora.post_release_inapp_analysis_brand		
		select
			var_date report_date, 
			device_brand,
			
			count(distinct case when event_name='in_app_message_received' and tbl2.mobile_no is null then mobile_no else null end) below_41_inapp_received_users, 
			count(case when event_name='in_app_message_received' and tbl2.mobile_no is null then id else null end) below_41_inapp_received_events,
			count(distinct case when event_name='in_app_message_open' and tbl2.mobile_no is null then mobile_no else null end) below_41_inapp_open_users, 
			count(case when event_name='in_app_message_open' and tbl2.mobile_no is null then id else null end) below_41_inapp_open_events, 
			
			count(distinct case when event_name='in_app_message_received' and tbl2.mobile_no is not null then mobile_no else null end) in_41_inapp_received_users, 
			count(case when event_name='in_app_message_received' and tbl2.mobile_no is not null then id else null end) in_41_inapp_received_events,
			count(distinct case when event_name='in_app_message_open' and tbl2.mobile_no is not null then mobile_no else null end) in_41_inapp_open_users, 
			count(case when event_name='in_app_message_open' and tbl2.mobile_no is not null then id else null end) in_41_inapp_open_events
		from 
			(select mobile_no, id, event_name
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('in_app_message_received', 'in_app_message_open')
			) tbl1 
			
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
from data_vajapora.post_release_inapp_analysis_brand; 

select 
	report_date, 
	
	sum(case when device_brand='samsung' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_samsung, 
	sum(case when device_brand='samsung' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_samsung, 
	
	sum(case when device_brand='vivo' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_vivo, 
	sum(case when device_brand='vivo' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_vivo,
	
	sum(case when device_brand='OPPO' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_OPPO, 
	sum(case when device_brand='OPPO' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_OPPO,
	
	sum(case when device_brand='xiaomi' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_xiaomi, 
	sum(case when device_brand='xiaomi' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_xiaomi,
	
	sum(case when device_brand='HUAWEI' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_HUAWEI, 
	sum(case when device_brand='HUAWEI' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_HUAWEI,
	
	sum(case when device_brand='Symphony' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_Symphony, 
	sum(case when device_brand='Symphony' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_Symphony,
	
	sum(case when device_brand='realme' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_realme, 
	sum(case when device_brand='realme' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_realme,
	
	sum(case when device_brand='Redmi' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_Redmi, 
	sum(case when device_brand='Redmi' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_Redmi,
	
	sum(case when device_brand='Itel' then below_41_inapp_open_users*1.00/below_41_inapp_received_users else 0 end) below_41_inapp_receive_to_open_users_ratio_Itel, 
	sum(case when device_brand='Itel' then in_41_inapp_open_users*1.00/in_41_inapp_received_users else 0 end) in_41_inapp_receive_to_open_users_ratio_Itel
from data_vajapora.post_release_inapp_analysis_brand
group by 1
order by 1; 
	
-- for device-wise inbox
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