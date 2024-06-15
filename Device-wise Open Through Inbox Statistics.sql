/*
- Viz: 
- Data: 
	- https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=201114635
	- for 4.1: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=1243884438
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

do $$ 

declare 
	var_date date:='2021-11-02'::date; 
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
			where event_date=var_date
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

select *
from data_vajapora.device_wise_open_thru_inbox;

-- for 4.1
do $$ 

declare 
	var_date date:='2022-02-17'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.device_wise_open_thru_inbox_41
		where report_date=var_date; 
	
		-- sequenced events of merchants, filtered for necessary events
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
			
			inner join 
			
			(select distinct mobile_no
			from tallykhata.tk_power_users_10
			where report_date=var_date
			) tbl2 using(mobile_no)
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
		insert into data_vajapora.device_wise_open_thru_inbox_41
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
		if var_date='2022-02-20'::date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.device_wise_open_thru_inbox_41
order by 1;

