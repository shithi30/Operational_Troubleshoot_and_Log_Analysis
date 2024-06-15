/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): This change is still to be incorporated to the main function. 
*/

select mobile_no, created_datetime, max_id, date_sequence+case when max_date_sequence is null then 0 else max_date_sequence end date_sequence, year_month, week_no     
from 
	(select distinct mobile_no, created_datetime, 0 max_id, 1 date_sequence, to_char(created_datetime, 'YYYY-MM-01') year_month, left(date_trunc('week', created_datetime::date)::text, 10) week_no        
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime=current_date -- drop data for a few days in the past and generate anew by iteration on this date
	) tbl1 
	
	left join 
	
	(select mobile_no, max(date_sequence) max_date_sequence
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	group by 1
	) tbl2 using(mobile_no); 

-- sample implementation
do $$

declare
	var_date date:=current_date-7;
begin
	raise notice 'New OP goes below:'; 

	delete from data_vajapora.tallykhata_user_date_sequence_final_temp_20211006
	where event_date>=var_date; 

	loop
		insert into data_vajapora.tallykhata_user_date_sequence_final_temp_20211006
		select mobile_no, event_date, max_id, date_sequence+case when max_date_sequence is null then 0 else max_date_sequence end date_sequence, year_month, week_no     
		from 
			(select distinct mobile_no, event_date, 0 max_id, 1 date_sequence, to_char(event_date, 'YYYY-MM-01') year_month, left(date_trunc('week', event_date::date)::text, 10) week_no        
			from tallykhata.event_transacting_fact
			where event_date=var_date 
			) tbl1 
			
			left join 
			
			(select mobile_no, max(date_sequence) max_date_sequence
			from data_vajapora.tallykhata_user_date_sequence_final_temp_20211006
			group by 1
			) tbl2 using(mobile_no); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$; 
