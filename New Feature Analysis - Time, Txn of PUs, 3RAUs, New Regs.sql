-- for sheet: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1981516415
do $$

declare
	var_date date:=current_date-15;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.new_feature_usage
		where report_date=var_date; 
	
		-- new cash txns of the day 
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select distinct mobile_no 
		from tallykhata.tallykhata_fact_info_final 
		where 
			created_datetime=var_date
			and txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_ADJUSTMENT'); 
			
		-- new regs (to the new version) 
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select mobile_number mobile_no
		from public.register_usermobile 
		where created_at>='2021-09-29 18:52:00';
			
		-- 3RAUs of the day
		drop table if exists data_vajapora.help_c;
		create table data_vajapora.help_c as
		select mobile_no
		from tallykhata.regular_active_user_event
		where 
			rau_category=3 
			and report_date::date=var_date; 
			
		-- PUs of the day
		drop table if exists data_vajapora.help_d;
		create table data_vajapora.help_d as
		select distinct mobile_no
		from tallykhata.tk_power_users_10
		where report_date=var_date; 
		
		-- new feature usage metrics
		insert into data_vajapora.new_feature_usage
		select 
			var_date report_date,	
			count(tbl1.mobile_no) new_feature_users,
			count(tbl2.mobile_no) new_feature_users_new_merchants,
			count(tbl3.mobile_no) new_feature_users_3raus,
			count(tbl4.mobile_no) new_feature_users_pus
		from 
			data_vajapora.help_a tbl1 
			left join 
			data_vajapora.help_b tbl2 using(mobile_no)
			left join 
			data_vajapora.help_c tbl3 using(mobile_no)
			left join 
			data_vajapora.help_d tbl4 using(mobile_no); 
			
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$; 

do $$

declare
	var_date date:=current_date-15;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.cash_cred_pu_temp
		where report_date=var_date; 
	
		-- PUs of the day
		drop table if exists data_vajapora.help_d;
		create table data_vajapora.help_d as
		select distinct mobile_no
		from tallykhata.tk_power_users_10
		where report_date=var_date; 
		
		insert into data_vajapora.cash_cred_pu_temp
		select var_date report_date, *
		from 
			(-- cash TRV of PUs
			select sum(cleaned_amount) pu_cash_trv
			from 
				(select mobile_no, cleaned_amount
				from tallykhata.tallykhata_fact_info_final 
				where 
					txn_type in('MALIK_NILO', 'MALIK_DILO', 'EXPENSE', 'CASH_PURCHASE', 'CASH_SALE', 'CASH_ADJUSTMENT')
					and created_datetime=var_date
				) tbl1 
			
				inner join 
				
				data_vajapora.help_d tbl2 using(mobile_no)
			) tbl1,
			
			(-- credit TRV of PUs
			select sum(cleaned_amount) pu_credit_trv
			from 
				(select mobile_no, cleaned_amount
				from tallykhata.tallykhata_fact_info_final 
				where 
					txn_type in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'CREDIT_PURCHASE')
					and created_datetime=var_date
				) tbl1 
			
				inner join 
				
				data_vajapora.help_d tbl2 using(mobile_no)
			) tbl2;
			
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$;  

select *
from 
	data_vajapora.new_feature_usage tbl1 
	inner join 
	data_vajapora.cash_cred_pu_temp tbl2 using(report_date)
where 
	new_feature_users!=0
	and report_date>='2021-09-29'
order by 1;

