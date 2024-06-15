/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2087723112
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
	var_date date:='16-Apr-22'::date; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select 
		mobile_no, 
		case 
			when device_brand in
				(select device_brand 
				from 
					(select lower(device_brand) device_brand, count(mobile) users 
					from public.registered_users 
					where device_status='active'
					group by 1
					order by 2 desc 
					limit 10
					) tbl1 
				) 
				then device_brand 
		end device_brand	
	from 
		(select mobile mobile_no, max(id) id
		from public.registered_users 
		where device_status='active'
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, lower(device_brand) device_brand
		from public.registered_users 
		) tbl2 using(id); 

	loop
		delete from data_vajapora.inbox_rec_analysis
		where report_date=var_date; 
	
		insert into data_vajapora.inbox_rec_analysis
		select var_date report_date, device_brand, count(distinct mobile_no) merchants_received_msg
		from 
			(select distinct mobile_no
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				created_date=var_date
				and event_name in('inbox_message_received')
			) tbl1 
			
			left join 
				
			data_vajapora.help_a tbl2 using(mobile_no)
		group by 1, 2; 
				
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.inbox_rec_analysis;