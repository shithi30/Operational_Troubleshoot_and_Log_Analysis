/*
- Viz: https://docs.google.com/spreadsheets/d/1YqISwXgjHF0atxM6F7NTLdT1YFywmlmlxQD6XRW0j8s/edit#gid=0
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
	Marketing Team Changes some configuration in the Google Ad link:
	Changes:
	1. TG selected as - clients who most likely to open the app.
	2. Evaluate them using firebase console.
	They made this change on 18th July, 2021
	To measure the impact of this change we needs to look into.
	1. New download vs registration trend.
	2. Registration vs 1st DAU trend.
	3. Registration vs 3RAU conversation trend with in 1-week
	4. Registration vs PU conversation trend with in 1st 10 days
	5. Registration vs 1st 7 days time spend trend 
	All of these analysis needs to be comparative.
	Two time range: 18 - 28 July, 2021 vs 8 to 18 July, 2021 vs 18 - 28 June, 2021
*/

do $$

declare 
	var_date date:='2021-06-01';
begin
	raise notice 'New OP goes below: '; 
	
	loop
		delete from data_vajapora.help_x
		where reg_date=var_date; 
	
		insert into data_vajapora.help_x 
		select 
			reg_date, 
			count(tbl1.mobile_no) reg_on_date,
			count(rau3_mobile_no) rau3_within_7_days,
			count(pu_mobile_no) pu_within_10_days,
			avg(first_week_sec_spent)/60.00 mins_spent_within_7_days
		from 
			(select date(created_at) reg_date, mobile_number mobile_no 
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl1 
			
			left join 
			
			(select distinct mobile_no rau3_mobile_no
			from tallykhata.regular_active_user_event
			where 
				rau_category=3
				and report_date::date>=var_date::date and report_date::date<=var_date::date+6
			) tbl2 on(tbl1.mobile_no=tbl2.rau3_mobile_no)
			
			left join 
				
			(select distinct mobile_no pu_mobile_no
			from tallykhata.tk_power_users_10
			where report_date>=var_date and report_date<=var_date::date+9
			) tbl3 on(tbl1.mobile_no=tbl3.pu_mobile_no)
			
			left join 
			
			(select mobile_no, sum(sec_with_tk) first_week_sec_spent
			from tallykhata.daily_times_spent_individual_data
			where event_date>=var_date and event_date<=var_date::date+6
			group by 1
			) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
		group by 1;
	
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop;
end $$; 

do $$

declare
	var_date date:='2021-06-01';
begin
	raise notice 'New OP goes below:'; 
		
	loop
		delete from data_vajapora.help_y
		where reg_date=var_date; 
		
		insert into data_vajapora.help_y
		select 
			reg_date, 
			count(distinct tbl1.mobile_no) reg_on_date,
			count(distinct first_dau_mobile_no) first_dau,
			count(distinct txn_mobile_no) first_txn_dau,
			count(auto_id) first_dau_trt_tacs
		from 
			(select date(created_at) reg_date, mobile_number mobile_no 
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl1 
			
			left join 
				
			(select mobile_no first_dau_mobile_no
			from tallykhata.tallykhata_user_date_sequence_final
			where event_date=var_date
			) tbl2 on(tbl1.mobile_no=tbl2.first_dau_mobile_no)
			
			left join 
			
			(select mobile_no txn_mobile_no, auto_id
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl3 on(tbl1.mobile_no=tbl3.txn_mobile_no)
		group by 1; 
	
		raise notice 'Data generated for %', var_date; 
		
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop;
end $$;	

/*
truncate table data_vajapora.help_x; 

select *
from data_vajapora.help_x;

truncate table data_vajapora.help_y;

select *
from data_vajapora.help_y;

select tbl1.*, first_dau, first_txn_dau, first_dau_trt_tacs
from 
	data_vajapora.help_x tbl1 
	inner join 
	data_vajapora.help_y tbl2 using(reg_date)
order by 1; 
*/
