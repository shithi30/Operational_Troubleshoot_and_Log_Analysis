-- dashboard data populated from: tallykhata.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard
-- function: look up from sheet
-- problem found in DAU and PU graphs

-- fix for DAU
do $$

declare

	mobile varchar;
	c_date varchar;
	v_error_msg text;
	arow record;
	equal_date varchar;
	v_from_date varchar;
	v_to_date varchar;
	sql_statement text;
	var_date date:=current_date-8; 

begin
	raise notice 'Programme execution started...%s',now();

	delete from tallykhata.daily_active_user_data
	where report_date>=var_date;
		
	-- regenerating last 8 days' data
	loop
		insert into tallykhata.daily_active_user_data
		select *
		from 
			(-- portion existing previously
			select
				*
			from 
				(
					select 
						ff.created_date as report_date,
						'sync_event' as category,
						count(distinct ff.mobile_no) as total_active_user,
						0 as total_active_user_did_txn,
						0 as total_active_user_did_cs_add,
						0 as nvu_total_active_user_did_txn,
						0 as nvu_total_active_user_did_cs_add,
						0 as total_trt,
						0 as total_cs_add,
						0 as total_ttv,
						0 as total_original_trv,
						0 as total_cleaned_trv,
						0 as total_cleaned_trv_2,
						0 as account_count,
						0 as cash_txn_count, 
						0 as credit_txn_count,
						0 as total_txn_count
					from tallykhata.tallykhata_sync_event_fact_final as ff
					where ff.created_date=var_date
					group by ff.created_date
			
			union all 
				
					select 
						f.created_datetime as report_date,
						'(DAU) Verified' as category,
						count(distinct f.mobile_no) as total_active_user,
						count(distinct case when f.entry_type = 1 then f.mobile_no end) as total_active_user_did_txn,
						count(distinct case when f.entry_type = 2 then f.mobile_no end) as total_active_user_did_cs_add,
						0 as nvu_total_active_user_did_txn,
						0 as nvu_total_active_user_did_cs_add,
						count(case when f.entry_type =1 then f.auto_id end) as total_trt,
						count(case when f.entry_type =2 then f.auto_id end) as total_cs_add,
						count(auto_id) as total_ttv,
						sum(f.input_amount) as total_original_trv,
						sum(f.cleaned_amount) as total_cleaned_trv,
						sum(f.cleaned_amount_2) as total_cleaned_trv_2,
						0 as account_count,
						0 as cash_txn_count, 
						0 as credit_txn_count,
						0 as total_txn_count
					from tallykhata.tallykhata_fact_info_final as f
					where f.created_datetime=var_date
					group by f.created_datetime
				
			union all 
			
					select 
						tbl_1.created_at::date as report_date,
						'Non Verified' as category,
						count(distinct tbl_1.mobile_number) as total_active_user,
						0 as total_active_user_did_txn,
						0 as total_active_user_did_cs_add,
						count(distinct case when tbl_1.account_count >= 1 then tbl_1.mobile_number end) as nvu_total_active_user_did_txn,
						count(distinct case when tbl_1.total_txn_count >= 1 then tbl_1.mobile_number end) as nvu_total_active_user_did_cs_add,
						0 as  total_trt,
						0 as total_cs_add,
						0 as total_ttv,
						0 as total_original_trv,
						0 as total_cleaned_trv,
						0 as total_cleaned_trv_2,
						sum(tbl_1.account_count) as account_count,
						sum(tbl_1.cash_txn_count) as cash_txn_count, 
						sum(tbl_1.credit_txn_count) as credit_txn_count,
						sum(tbl_1.total_txn_count) as total_txn_count
					from (
					
						select 
							ss.mobile_number
							,ss.created_at::date
							,ss.account_count
							,ss.cash_txn_count 
							,ss.credit_txn_count
							,ss.total_txn_count
						from public.user_summary as ss 
						left join public.register_usermobile as i on ss.mobile_number = i.mobile_number
						where i.mobile_number is null 
						and ss.created_at::date=var_date
					) as tbl_1 group by tbl_1.created_at::date
				) as tbl_3
				
				union all 
				
				select 
					tbl_4.report_date,
					'db_plus_event' as category,
					count(distinct tbl_4.mobile_no) as total_active_user,
					0 as total_active_user_did_txn,
					0 as total_active_user_did_cs_add,
					0 as nvu_total_active_user_did_txn,
					0 as nvu_total_active_user_did_cs_add,
					0 as  total_trt,
					0 as total_cs_add,
					0 as total_ttv,
					0 as total_original_trv,
					0 as total_cleaned_trv,
					0 as total_cleaned_trv_2,
					0 as account_count,
					0 as cash_txn_count, 
					0 as credit_txn_count,
					0 as total_txn_count
				from 
					(
						select 
							distinct ff.created_date as report_date,
							ff.mobile_no
						from tallykhata.tallykhata_sync_event_fact_final as ff
						where ff.created_date=var_date
				
					union all 
					
						select 
							distinct f.created_datetime as report_date,
							f.mobile_no
						from tallykhata.tallykhata_fact_info_final as f
						where f.created_datetime=var_date
					) as tbl_4 group by tbl_4.report_date
			) tbl1 
			
			left join 
			
			(-- portion added by Shithi Maitra
			select 
				var_date report_date, 
				'(DAU) Verified' category,
				-- count(mobile_no) no_of_txn_user_for_breakdown,
				count(case when if_cash_txn=0 and if_cred_txn>0 then mobile_no else null end) no_of_credit_txn_user,
				count(case when if_cash_txn>0 and if_cred_txn=0 then mobile_no else null end) no_of_cash_txn_user,
				count(case when if_cash_txn>0 and if_cred_txn>0 then mobile_no else null end) no_of_cred_and_cash_txn_user,
				count(case when if_cash_txn=0 and if_cred_txn=0 then mobile_no else null end) no_of_tacs_txn_user
			from 
				(select 
					mobile_no,
					count(case when txn_type in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'CREDIT_PURCHASE') then auto_id else null end) if_cred_txn,
					count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE', 'EXPENSE', 'DIGITAL_SALE') then auto_id else null end) if_cash_txn,
					count(case when txn_type in('Add Customer', 'Add Supplier') then auto_id else null end) if_tacs_txn
				from tallykhata.tallykhata_fact_info_final 
				where created_datetime=var_date
				group by 1
				) tbl1
			
			union all
			
			select 
				var_date report_date, 
				'Non Verified' category,
				-- count(mobile_no) no_of_txn_user_for_breakdown,
				count(case when cash_txn_count=0 and credit_txn_count>0 then mobile_no else null end) no_of_credit_txn_user,
				count(case when cash_txn_count>0 and credit_txn_count=0 then mobile_no else null end) no_of_cash_txn_user,
				count(case when cash_txn_count>0 and credit_txn_count>0 then mobile_no else null end) no_of_cred_and_cash_txn_user,
				count(case when cash_txn_count=0 and credit_txn_count=0 then mobile_no else null end) no_of_tacs_txn_user
			from 
				(select mobile_number mobile_no, sum(cash_txn_count) cash_txn_count, sum(credit_txn_count) credit_txn_count
				from public.user_summary tbl1
					left join public.register_usermobile tbl2 using(mobile_number)
				where 
					tbl2.mobile_number is null 
					and tbl1.created_at::date=var_date
				group by 1
				) tbl1
			) tbl2 using(report_date, category); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

end $$;

-- fix for PU
delete from tallykhata.distinct_pu_user where report_date >= current_date - 5;
insert into tallykhata.distinct_pu_user
select t1.*
from
(
	select
		*,
		row_number() over(partition by mobile_no,report_date) as row_no
	from tallykhata.tallykhata.tk_power_users_10
	where report_date >= current_date - 5
) as t1 where t1.row_no = 1;

delete from tallykhata.tallykhata.tallykhata_pu_user_run_rate where report_date::date >= current_date-7;
insert into tallykhata.tallykhata.tallykhata_pu_user_run_rate
select
	s.mobile_no
	,s.report_date
	,count(f.auto_id) as total_activity 
	,sum(f.cleaned_amount) as total_amount 
from tallykhata.distinct_pu_user as s 
inner join tallykhata.tallykhata.tallykhata_fact_info_final as f on s.mobile_no = f.mobile_no 
and f.created_datetime::date = s.report_date and f.created_datetime::date >= current_date -7 
and f.created_datetime::date < current_date
group by s.mobile_no, s.report_date;

-- combine and commit fixes
truncate table tallykhata.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard;
insert into tallykhata.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard
select 
	s.to_rau_date
	,'3RAU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from tallykhata.tallykhata_user_run_rate as s where s.rau_category = 3
group by s.to_rau_date

union all 

select 
	s.to_rau_date
	,'10RAU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from tallykhata.tallykhata_user_run_rate as s where s.rau_category = 10
group by s.to_rau_date

union all 

select 
	report_date as dau_date,
	'DAU TTV/user' as category,
	round((total_trt/total_active_user_did_txn::float)::numeric,2) as ttv_rate,
	(total_cleaned_trv/total_active_user_did_txn)::int as tpv_rate
from tallykhata.daily_active_user_data where category ='(DAU) Verified' and report_date >= '2020-07-01'

union all 

select 
	s.to_fau_date
	,'FAU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile))::int as total_tpv
from tallykhata.tallykhata_fau_user_run_rate as s
group by s.to_fau_date

union all

select 
	s.report_date
	,'PU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from tallykhata.tallykhata_pu_user_run_rate as s
group by s.report_date;

