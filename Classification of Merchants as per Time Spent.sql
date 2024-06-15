/*
- Viz: https://docs.google.com/spreadsheets/d/10SHDZJHR9romgn8PcM0iok4gR7eoGeRpbQCj7ruqpK4/edit#gid=0
- Data: 
- Function: 
- Table: data_vajapora.merchant_cls_mins_spent
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

do $$

declare
	var_date date:=current_date-30;
begin 

	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.merchant_cls_mins_spent
		where class_date=var_date;
	
		-- merchants who registered at least 30 days back, with how many minutes they have spent in lifetime
		drop table if exists data_vajapora.help_a;	
		create table data_vajapora.help_a as
		select mobile_no, reg_date, case when lft_mins_spent is null then 0 else lft_mins_spent end lft_mins_spent
		from 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile 
			where date(created_at)<var_date-30
			) tbl1 
			
			left join 
			
			(select mobile_no, floor(sum(sec_with_tk)/60.00) lft_mins_spent
			from tallykhata.daily_times_spent_individual_data
			where event_date<var_date
			group by 1
			) tbl2 using(mobile_no); 

		-- merchants' given times in the last 14 days
		drop table if exists data_vajapora.help_b;	
		create table data_vajapora.help_b as
		select mobile_no, event_date, sec_with_tk
		from tallykhata.daily_times_spent_individual_data
		where event_date>=var_date-14 and event_date<var_date;
		
		-- classification of merchants
		insert into data_vajapora.merchant_cls_mins_spent
		select var_date class_date, merchant_class, count(mobile_no) merchants
		from 
			(select
				*,
				case
					when(lft_mins_spent=0) then 'init'
					when(week_2_mins_spent=0) then 'churned'
					when(week_1_mins_spent>=18 and week_2_mins_spent>=18) then 'regular' 
					when(week_1_mins_spent<=week_2_mins_spent) then 'improving'
					when(week_1_mins_spent>week_2_mins_spent) then 'churning'
					else 'none'
				end merchant_class
			from 
				(select 
					mobile_no, lft_mins_spent,
					sum(case when event_date>=var_date-14 and event_date<var_date-7 then sec_with_tk else 0 end)/60.00 week_1_mins_spent,
					sum(case when event_date>=var_date-7 and event_date<var_date then sec_with_tk else 0 end)/60.00 week_2_mins_spent
				from 
					data_vajapora.help_a tbl1 
					left join 
					data_vajapora.help_b tbl2 using(mobile_no)
				group by 1, 2
				) tbl1
			) tbl2
		group by 1, 2
		order by 3 desc; 
				
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;

	end loop; 

end $$;

/*	
truncate table data_vajapora.merchant_cls_mins_spent; 

select *
from data_vajapora.merchant_cls_mins_spent; 

select 
	class_date, 
	sum(case when merchant_class='init' then merchants else 0 end) init,
	sum(case when merchant_class='improving' then merchants else 0 end) improving,
	sum(case when merchant_class='regular' then merchants else 0 end) regular,
	sum(case when merchant_class='churning' then merchants else 0 end) churning,
	sum(case when merchant_class='churned' then merchants else 0 end) churned
from data_vajapora.merchant_cls_mins_spent
group by 1
order by 1; 
*/
