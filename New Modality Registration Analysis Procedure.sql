/*
- Viz: https://docs.google.com/spreadsheets/d/1YqISwXgjHF0atxM6F7NTLdT1YFywmlmlxQD6XRW0j8s/edit#gid=0
- Data: 
- Function: data_vajapora.fn_new_modality_reg_analysis()
- Table: data_vajapora.new_mod_reg
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): >> Production Function name: tallykhata.fn_tk_registration_to_acquisition_analysis()
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_new_modality_reg_analysis()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysing impact of new modality registrations on 3RAU, PU, first DAU and time spent
Auxiliary data table(s) : data_vajapora.new_mod_reg_rau_pu_time, data_vajapora.new_mod_reg_fst_dau
Target data table(s)    : data_vajapora.new_mod_reg
*/

declare 
	var_date date;
begin
	
	select max(reg_date)-6 into var_date
	from data_vajapora.new_mod_reg;

	-- eliminating backdated info.
	delete from data_vajapora.new_mod_reg_rau_pu_time 
	where reg_date>=var_date;
	delete from data_vajapora.new_mod_reg_fst_dau 
	where reg_date>=var_date;
	
	raise notice 'New OP goes below: '; 
	
	loop
		-- generating 3RAU/PU-data
		insert into data_vajapora.new_mod_reg_rau_pu_time 
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
	
		-- generating first DAU-data
		insert into data_vajapora.new_mod_reg_fst_dau
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
	
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop;

	-- generating combined data
	drop table if exists data_vajapora.new_mod_reg; 
	create table data_vajapora.new_mod_reg as
	select 
		tbl1.*, 
		first_dau, first_txn_dau, first_dau_trt_tacs, 
		rau3_within_7_days*1.00/tbl1.reg_on_date rau3_within_7_days_pct, 
		pu_within_10_days*1.00/tbl1.reg_on_date pu_within_10_days_pct,
		first_txn_dau*1.00/tbl1.reg_on_date first_txn_dau_pct
	from 
		data_vajapora.new_mod_reg_rau_pu_time tbl1 
		inner join 
		data_vajapora.new_mod_reg_fst_dau tbl2 using(reg_date)
	order by 1; 

END;
$function$
;

/*
select data_vajapora.fn_new_modality_reg_analysis();

select * 
from data_vajapora.new_mod_reg; 
*/
