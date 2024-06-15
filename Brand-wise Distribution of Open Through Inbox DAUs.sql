/*
- Viz: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=685565300
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

do $$ 

declare 
	var_date date:=current_date-3;
begin 
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.inbox_open_reduction_analysis_2
		where report_date=var_date; 
	
		-- brand of the active device on var_date
		drop table if exists data_vajapora.temp_c; 
		create table data_vajapora.temp_c as
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
			
			(select id, device_brand
			from public.registered_users 
			) tbl2 using(id); 

		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
		
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.temp_b;
		create table data_vajapora.temp_b as
		select tbl1.mobile_no, tbl3.id
		from 
			data_vajapora.temp_a tbl1
			inner join 
			data_vajapora.temp_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			left join 
			(select mobile_no, min(id) id 
			from data_vajapora.temp_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		
		-- necessary statistics
		insert into data_vajapora.inbox_open_reduction_analysis_2
		select var_date report_date, mobile_no first_open_through_inbox_merchants, device_brand
		from 
			data_vajapora.temp_b tbl1
			left join 
			data_vajapora.temp_c tbl2 using(mobile_no)
		where id is not null; 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-00 then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.inbox_open_reduction_analysis_2; 

-- identify top brands
select device_brand, count(*) merchants, count(*)*1.00/(select count(*) from public.registered_users) merchants_pct
from public.registered_users 
group by 1
order by 2 desc; 

select 
	report_date, 
	count(first_open_through_inbox_merchants) first_open_through_inbox_merchants,
	count(case when device_brand='samsung' then first_open_through_inbox_merchants else null end) open_through_inbox_samsung, 
	count(case when device_brand='vivo' then first_open_through_inbox_merchants else null end) open_through_inbox_vivo, 
	count(case when device_brand='OPPO' then first_open_through_inbox_merchants else null end) open_through_inbox_oppo, 
	count(case when device_brand in('xiaomi', 'Xiaomi') then first_open_through_inbox_merchants else null end) open_through_inbox_xiaomi, 
	count(case when device_brand='HUAWEI' then first_open_through_inbox_merchants else null end) open_through_inbox_huawei, 
	count(case when device_brand='Symphony' then first_open_through_inbox_merchants else null end) open_through_inbox_symphony,
	count(case when device_brand='realme' then first_open_through_inbox_merchants else null end) open_through_inbox_realme, 
	count(case when device_brand='Redmi' then first_open_through_inbox_merchants else null end) open_through_inbox_redmi, 
	count(case when device_brand='Itel' then first_open_through_inbox_merchants else null end) open_through_inbox_itel
from data_vajapora.inbox_open_reduction_analysis_2
group by 1
order by 1; 
