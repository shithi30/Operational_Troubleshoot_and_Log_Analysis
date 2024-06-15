/* run daily */

-- populating data till yesterday from a given date
do $$ 

declare 
	var_date date:=current_date-20; 
begin
	raise notice 'New OP goes below:'; 

	delete from tallykhata.tallykhata_overall_progress_bar_v2
	where created_datetime>=var_date; 

	loop
		raise notice 'Generating data for: %', var_date; 
		insert into tallykhata.tallykhata_overall_progress_bar_v2
		select 
			created_datetime, 	
			sum(cleaned_amount) tpv, 
			count(distinct case when entry_type=1 then auto_id else null end) ttv,
			count(distinct case when txn_type='Add Customer' then auto_id else null end) total_customer_add,
			count(distinct case when txn_type='Add Supplier' then auto_id else null end) total_supplier_add, 
			count(distinct case when entry_type=2 then auto_id else null end) total_cust_supp_add, 
			count(distinct mobile_no) active_non_raus
		from 
			(select created_datetime, mobile_no, entry_type, txn_type, cleaned_amount, auto_id
			from tallykhata.tallykhata_fact_info_final 
			where date(created_datetime)=var_date
			) tbl1
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tk_power_users_10 
			where report_date=var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null
		group by 1
		order by 1 asc; 
	
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 

end $$;
