/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=309970650
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any):
	We have analyzed device-wise DAU-to-'open through inbox notification' ratios from 01-Dec-2021. 
	We have not noticed any anomaly regarding Huawei devices. Huawei devices' trend seem to comply with the generic trends. 
	The decrease in the overall time spent may be due to a fall in less time spent in inbox. 
*/

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

-- device-wise DAU distribution
do $$ 

declare 
	var_date date:=current_date-7; 
begin 
	loop
		delete from data_vajapora.device_wise_daus 
		where report_date=var_date; 
	
		insert into data_vajapora.device_wise_daus
		select
			var_date report_date,
			count(distinct mobile_no) daus, 
			count(distinct case when device_brand='samsung' then mobile_no else null end) daus_samsung, 
			count(distinct case when device_brand='vivo' then mobile_no else null end) daus_vivo, 
			count(distinct case when device_brand='oppo' then mobile_no else null end) daus_oppo, 
			count(distinct case when device_brand='xiaomi' then mobile_no else null end) daus_xiaomi, 
			count(distinct case when device_brand='huawei' then mobile_no else null end) daus_huawei, 
			count(distinct case when device_brand='symphony' then mobile_no else null end) daus_symphony, 
			count(distinct case when device_brand='realme' then mobile_no else null end) daus_realme, 
			count(distinct case when device_brand='redmi' then mobile_no else null end) daus_redmi, 
			count(distinct case when device_brand='itel' then mobile_no else null end) daus_itel, 
			count(distinct case when device_brand='walton' then mobile_no else null end) daus_walton,
			count(distinct case when device_brand is null then mobile_no else null end) daus_others
		from 
			(-- DAUs
			select mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			
			union 
			
			select mobile_no 
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				date(created_date)=var_date
				and event_name not in('in_app_message_received','inbox_message_received')
				
			union 
				
			select ss.mobile_number mobile_no
			from 
				public.user_summary as ss 
				left join 
				public.register_usermobile as i on ss.mobile_number = i.mobile_number
			where 
				i.mobile_number is null 
				and ss.created_at::date=var_date
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

-- data to put
select 
	report_date, 
	open_through_inbox_merchants_samsung, daus_samsung, open_through_inbox_merchants_samsung*1.00/daus_samsung open_through_inbox_merchants_samsung_pct, 
	open_through_inbox_merchants_vivo, daus_vivo, open_through_inbox_merchants_vivo*1.00/daus_vivo open_through_inbox_merchants_vivo_pct, 
	open_through_inbox_merchants_oppo, daus_oppo, open_through_inbox_merchants_oppo*1.00/daus_oppo open_through_inbox_merchants_oppo_pct, 
	open_through_inbox_merchants_xiaomi, daus_xiaomi, open_through_inbox_merchants_xiaomi*1.00/daus_xiaomi open_through_inbox_merchants_xiaomi_pct, 
	open_through_inbox_merchants_huawei, daus_huawei, open_through_inbox_merchants_huawei*1.00/daus_huawei open_through_inbox_merchants_huawei_pct, 
	open_through_inbox_merchants_symphony, daus_symphony, open_through_inbox_merchants_symphony*1.00/daus_symphony open_through_inbox_merchants_symphony_pct, 
	open_through_inbox_merchants_realme, daus_realme, open_through_inbox_merchants_realme*1.00/daus_realme open_through_inbox_merchants_realme_pct, 
	open_through_inbox_merchants_redmi,	daus_redmi, open_through_inbox_merchants_redmi*1.00/daus_redmi open_through_inbox_merchants_redmi_pct, 
	open_through_inbox_merchants_itel, daus_itel, open_through_inbox_merchants_itel*1.00/daus_itel open_through_inbox_merchants_itel_pct, 
	open_through_inbox_merchants_walton, daus_walton, open_through_inbox_merchants_walton*1.00/daus_walton open_through_inbox_merchants_walton_pct, 
	open_through_inbox_merchants_others, daus_others, open_through_inbox_merchants_others*1.00/daus_others open_through_inbox_merchants_others_pct
from 
	data_vajapora.device_wise_daus tbl1 
	inner join 
	data_vajapora.device_wise_open_thru_inbox tbl2 using(report_date) 
order by 1 desc; 

