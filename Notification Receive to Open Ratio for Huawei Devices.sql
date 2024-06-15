/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=1846929939
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

-- device-wise receive distribution
do $$ 

declare 
	var_date date:=current_date-7; 
begin 
	loop
		delete from data_vajapora.device_wise_received_merchants 
		where report_date=var_date; 
	
		insert into data_vajapora.device_wise_received_merchants
		select
			var_date report_date,
			count(distinct mobile_no) received_merchants, 
			count(distinct case when device_brand='samsung' then mobile_no else null end) received_merchants_samsung, 
			count(distinct case when device_brand='vivo' then mobile_no else null end) received_merchants_vivo, 
			count(distinct case when device_brand='oppo' then mobile_no else null end) received_merchants_oppo, 
			count(distinct case when device_brand='xiaomi' then mobile_no else null end) received_merchants_xiaomi, 
			count(distinct case when device_brand='huawei' then mobile_no else null end) received_merchants_huawei, 
			count(distinct case when device_brand='symphony' then mobile_no else null end) received_merchants_symphony, 
			count(distinct case when device_brand='realme' then mobile_no else null end) received_merchants_realme, 
			count(distinct case when device_brand='redmi' then mobile_no else null end) received_merchants_redmi, 
			count(distinct case when device_brand='itel' then mobile_no else null end) received_merchants_itel, 
			count(distinct case when device_brand='walton' then mobile_no else null end) received_merchants_walton,
			count(distinct case when device_brand is null then mobile_no else null end) received_merchants_others
		from 
			(-- received stats
			select mobile_no
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name='inbox_message_received'
				and event_date=var_date
			) tbl1 
			
			left join 
						
			(-- brands
			select mobile_no, device_brand
			from 
				(select mobile mobile_no, max(id) id
				from public.registered_users 
				where device_status='active'
				group by 1
				) tbl1 
				
				inner join 
				
				(select 
					id, 
					case 
						when lower(device_brand)='samsung' then 'samsung'
						when lower(device_brand)='vivo' then 'vivo'
						when lower(device_brand)='oppo' then 'oppo'
						when lower(device_brand)='xiaomi' then 'xiaomi'
						when lower(device_brand)='huawei' then 'huawei'
						when lower(device_brand)='symphony' then 'symphony'
						when lower(device_brand)='realme' then 'realme'
						when lower(device_brand)='redmi' then 'redmi'
						when lower(device_brand)='itel' then 'itel'
						when lower(device_brand)='walton' then 'walton'
					end device_brand
				from public.registered_users 
				) tbl2 using(id)
			) tbl2 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

-- device-wise open-through-inbox distribution
do $$ 

declare 
	var_date date:=current_date-7; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.device_wise_open_thru_inbox
		where report_date=var_date; 
	
		-- sequenced events of merchants, filtered for necessary events
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where created_date=var_date
			) tbl1 
		where event_name in('inbox_message_open', 'app_opened'); 
		
		-- all push-open cases, with first opens of the day identified 
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.mobile_no, tbl4.device_brand
		from 
			data_vajapora.help_a tbl1
			
			inner join 
			
			data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			
			left join 
			
			(select mobile_no, device_brand
			from 
				(select mobile mobile_no, max(id) id
				from public.registered_users 
				where device_status='active'
				group by 1
				) tbl1 
				
				inner join 
				
				(select 
					id, 
					case 
						when lower(device_brand)='samsung' then 'samsung'
						when lower(device_brand)='vivo' then 'vivo'
						when lower(device_brand)='oppo' then 'oppo'
						when lower(device_brand)='xiaomi' then 'xiaomi'
						when lower(device_brand)='huawei' then 'huawei'
						when lower(device_brand)='symphony' then 'symphony'
						when lower(device_brand)='realme' then 'realme'
						when lower(device_brand)='redmi' then 'redmi'
						when lower(device_brand)='itel' then 'itel'
						when lower(device_brand)='walton' then 'walton'
						else 'others'
					end device_brand
				from public.registered_users 
				) tbl2 using(id)
			) tbl4 on(tbl2.mobile_no=tbl4.mobile_no)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened';
		
		-- necessary statistics
		insert into data_vajapora.device_wise_open_thru_inbox
		select
			var_date report_date,
			count(distinct mobile_no) open_through_inbox_merchants, 
			count(distinct case when device_brand='samsung' then mobile_no else null end) open_through_inbox_merchants_samsung, 
			count(distinct case when device_brand='vivo' then mobile_no else null end) open_through_inbox_merchants_vivo, 
			count(distinct case when device_brand='oppo' then mobile_no else null end) open_through_inbox_merchants_oppo, 
			count(distinct case when device_brand='xiaomi' then mobile_no else null end) open_through_inbox_merchants_xiaomi, 
			count(distinct case when device_brand='huawei' then mobile_no else null end) open_through_inbox_merchants_huawei, 
			count(distinct case when device_brand='symphony' then mobile_no else null end) open_through_inbox_merchants_symphony, 
			count(distinct case when device_brand='realme' then mobile_no else null end) open_through_inbox_merchants_realme, 
			count(distinct case when device_brand='redmi' then mobile_no else null end) open_through_inbox_merchants_redmi, 
			count(distinct case when device_brand='itel' then mobile_no else null end) open_through_inbox_merchants_itel, 
			count(distinct case when device_brand='walton' then mobile_no else null end) open_through_inbox_merchants_walton,
			count(distinct case when device_brand='others' then mobile_no else null end) open_through_inbox_merchants_others
		from data_vajapora.help_b; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

-- data to put
select 
	report_date, 
	open_through_inbox_merchants_samsung, received_merchants_samsung, open_through_inbox_merchants_samsung*1.00/received_merchants_samsung open_through_inbox_merchants_samsung_pct, 
	open_through_inbox_merchants_vivo, received_merchants_vivo, open_through_inbox_merchants_vivo*1.00/received_merchants_vivo open_through_inbox_merchants_vivo_pct, 
	open_through_inbox_merchants_oppo, received_merchants_oppo, open_through_inbox_merchants_oppo*1.00/received_merchants_oppo open_through_inbox_merchants_oppo_pct, 
	open_through_inbox_merchants_xiaomi, received_merchants_xiaomi, open_through_inbox_merchants_xiaomi*1.00/received_merchants_xiaomi open_through_inbox_merchants_xiaomi_pct, 
	open_through_inbox_merchants_huawei, received_merchants_huawei, open_through_inbox_merchants_huawei*1.00/received_merchants_huawei open_through_inbox_merchants_huawei_pct, 
	open_through_inbox_merchants_symphony, received_merchants_symphony, open_through_inbox_merchants_symphony*1.00/received_merchants_symphony open_through_inbox_merchants_symphony_pct, 
	open_through_inbox_merchants_realme, received_merchants_realme, open_through_inbox_merchants_realme*1.00/received_merchants_realme open_through_inbox_merchants_realme_pct, 
	open_through_inbox_merchants_redmi,	received_merchants_redmi, open_through_inbox_merchants_redmi*1.00/received_merchants_redmi open_through_inbox_merchants_redmi_pct, 
	open_through_inbox_merchants_itel, received_merchants_itel, open_through_inbox_merchants_itel*1.00/received_merchants_itel open_through_inbox_merchants_itel_pct, 
	open_through_inbox_merchants_walton, received_merchants_walton, open_through_inbox_merchants_walton*1.00/received_merchants_walton open_through_inbox_merchants_walton_pct, 
	open_through_inbox_merchants_others, received_merchants_others, open_through_inbox_merchants_others*1.00/received_merchants_others open_through_inbox_merchants_others_pct
from 
	data_vajapora.device_wise_received_merchants tbl1 
	inner join 
	data_vajapora.device_wise_open_thru_inbox tbl2 using(report_date) 
where report_date>='2021-12-01'::date
order by 1 desc; 
