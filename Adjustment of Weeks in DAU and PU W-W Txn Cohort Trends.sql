/*
- Viz: 
- Data: 
- Function: 
	- tallykhata.fn_pu_reg_week_info()
	- tallykhata.fn_dau_rau_users_registration_trend()
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/dQKnB
- Email thread: 
- Notes (if any): 
In W-W Cohort (TXN), We use ranges "W0-6	W7-12	W13-24	W25-48	W49-96	W96+" for SU charts.
We should use the same ranges for DAU and PU as well.
This will help in comparing and understanding.
*/

-- 3RAU
do $$ 

declare 
	var_date date:=current_date-7; 
begin 
	raise notice 'New OP goes below:'; 
	loop	
		delete from tallykhata.tallykhata_app_open_3_rau_users_registration_trend_2 
		where report_date=var_date;
	
		insert into tallykhata.tallykhata_app_open_3_rau_users_registration_trend_2
		select 
			report_date, 
			count(case when back_to_reg='Reg in 6 weeks' then mobile_no else null end) reg_in_6_weeks,
			count(case when back_to_reg='Reg in 12 weeks' then mobile_no else null end) reg_in_7_to_12_weeks,
			count(case when back_to_reg='Reg in 24 weeks' then mobile_no else null end) reg_in_13_to_24_weeks,
			count(case when back_to_reg='Reg in 48 weeks' then mobile_no else null end) reg_in_25_to_48_weeks,
			count(case when back_to_reg='Reg in 96 weeks' then mobile_no else null end) reg_in_49_to_96_weeks,
			count(case when back_to_reg='Reg in more than 96 weeks' then mobile_no else null end) reg_in_more_than_96_weeks
		from 
			(select 
				*, 
				case
					when report_date-reg_date<=42 then 'Reg in 6 weeks'
					when report_date-reg_date<=84 then 'Reg in 12 weeks'
					when report_date-reg_date<=168 then 'Reg in 24 weeks'
					when report_date-reg_date<=336 then 'Reg in 48 weeks'
					when report_date-reg_date<=672 then 'Reg in 96 weeks'
					else 'Reg in more than 96 weeks'
				end back_to_reg
			from 
				(select distinct report_date, mobile_no
				from tallykhata.regular_active_user_event
				where 
					rau_category=3
					and report_date=var_date
				) tbl1 
				
				inner join 
				
				(select mobile_number mobile_no, date(created_at) reg_date
				from public.register_usermobile 
				) tbl2 using(mobile_no)
			) tbl1
		group by 1; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select * 
from tallykhata.tallykhata_app_open_3_rau_users_registration_trend_2; 
