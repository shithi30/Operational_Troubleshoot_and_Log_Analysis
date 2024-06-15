/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1sxB47kgTp2T1W8JDBt46KFsG6BgC-5utdoox1W7T_vQ/edit#gid=508053401
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
pelam e, cash screen e CREDIT_SALE_RETURN thaka dorkar
Sir, we have checked the cases where users got blocked (but finally resolved) due to inadequate cash balance. 
In such cases, 3 txns: dilam, pelam and adjustment take up the same timestamp, as guessed. We found 1.71% (~ 170 merchants) such cash-users daily.
*/

-- modality 1: with the assumption of <=1 min gap between pelam, dilam
do $$

declare 
	var_date date:='2021-09-29';
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.forced_cash_entry_stats
		where report_date=var_date;
	
		insert into data_vajapora.forced_cash_entry_stats
		select var_date report_date, *
		from 
			(select count(distinct mobile_no) merchants_entered_cash
			from tallykhata.tallykhata_fact_info_final 
			where 
				txn_type in('MALIK_NILO', 'EXPENSE', 'CASH_PURCHASE', 'CASH_SALE', 'MALIK_DILO')
				and created_datetime=var_date
			) tbl1,
			
			(select count(distinct tbl1.mobile_no) merchants_forced_to_enter_cash
			from 
				(select mobile_no, created_timestamp, txn_type, row_number() over(partition by mobile_no order by created_timestamp asc) seq
				from tallykhata.tallykhata_fact_info_final 
				where created_datetime=var_date
				) tbl1 
				
				inner join 
					
				(select mobile_no, created_timestamp, txn_type, row_number() over(partition by mobile_no order by created_timestamp asc) seq
				from tallykhata.tallykhata_fact_info_final 
				where created_datetime=var_date
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			where 
				tbl1.txn_type in('CASH_SALE', 'MALIK_DILO')
				and 
				tbl2.txn_type in('MALIK_NILO', 'EXPENSE', 'CASH_PURCHASE')
				and
					 date_part(  'hour', tbl2.created_timestamp-tbl1.created_timestamp)*3600
					+date_part('minute', tbl2.created_timestamp-tbl1.created_timestamp)*60
					+date_part('second', tbl2.created_timestamp-tbl1.created_timestamp)=0
			) tbl2; 
				
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

-- modality 2 (more acceptatble): after knowing about simultaneous entries
do $$

declare 
	var_date date:='2021-09-29';
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.forced_cash_entry_stats
		where report_date=var_date;
	
		insert into data_vajapora.forced_cash_entry_stats
		select var_date report_date, *
		from 
			(select count(distinct mobile_no) merchants_entered_cash
			from tallykhata.tallykhata_fact_info_final 
			where 
				txn_type in('MALIK_NILO', 'EXPENSE', 'CASH_PURCHASE', 'CASH_SALE', 'MALIK_DILO')
				and created_datetime=var_date
			) tbl1,
			
			(select count(distinct tbl1.mobile_no) merchants_forced_to_enter_cash
			from 
				(select mobile_no, txn_timestamp, txn_type, row_number() over(partition by mobile_no order by txn_timestamp asc) seq
				from tallykhata.tallykhata_fact_info_final 
				where 
					txn_type in('MALIK_DILO', 'CASH_ADJUSTMENT')
					and created_datetime=var_date
				) tbl1 
				
				inner join 
					
				(select mobile_no, txn_timestamp, txn_type, row_number() over(partition by mobile_no order by txn_timestamp asc) seq
				from tallykhata.tallykhata_fact_info_final 
				where 
					txn_type in('MALIK_DILO', 'CASH_ADJUSTMENT')
					and created_datetime=var_date
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			where 
				tbl1.txn_type!=tbl2.txn_type
				and left(tbl1.txn_timestamp::text, 19)=left(tbl2.txn_timestamp::text, 19)
			) tbl2; 
				
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.forced_cash_entry_stats;

