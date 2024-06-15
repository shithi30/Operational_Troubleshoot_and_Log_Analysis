/*
- Viz: https://docs.google.com/spreadsheets/d/1RXgKF7FmiEq-oRMBB8SqYIdqvFZZxo73pcN8-eDdFmA/edit#gid=850824135
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

-- uninstalled in Oct-21 (from Mahmud)
select * from test.merchants_with_min_one_txn;
select * from test.merchants_with_no_txn;

-- necessary metrics
select *
from 
	(select min_event_date report_date, count(mobile_no) winback_and_app_open_with_min_1_previous_txn
	from 
		(select mobile_no, min(event_date) min_event_date
		from 
			tallykhata.tallykhata_user_date_sequence_final tbl1 
			inner join 
			test.merchants_with_min_one_txn tbl2 using(mobile_no)
		where event_date>='2021-10-18' and event_date<current_date
		group by 1
		) tbl1
	group by 1
	) tbl1 
	
	inner join 
	
	(select min_event_date report_date, count(mobile_no) winback_and_app_open_with_no_previous_txn
	from 
		(select mobile_no, min(event_date) min_event_date
		from 
			tallykhata.tallykhata_user_date_sequence_final tbl1 
			inner join 
			test.merchants_with_no_txn tbl2 using(mobile_no)
		where event_date>='2021-10-18' and event_date<current_date
		group by 1
		) tbl1
	group by 1
	) tbl2 using(report_date)
	
	inner join 
	
	(select min_event_date report_date, count(mobile_no) winback_and_txn_with_min_1_previous_txn
	from 
		(select mobile_no, min(created_datetime) min_event_date
		from 
			tallykhata.tallykhata_transacting_user_date_sequence_final tbl1 
			inner join 
			test.merchants_with_min_one_txn tbl2 using(mobile_no)
		where created_datetime>='2021-10-18' and created_datetime<current_date
		group by 1
		) tbl1
	group by 1
	) tbl3 using(report_date)
	
	inner join 
	
	(select min_event_date report_date, count(mobile_no) winback_and_txn_with_no_previous_txn
	from 
		(select mobile_no, min(created_datetime) min_event_date
		from 
			tallykhata.tallykhata_transacting_user_date_sequence_final tbl1 
			inner join 
			test.merchants_with_no_txn tbl2 using(mobile_no)
		where created_datetime>='2021-10-18' and created_datetime<current_date
		group by 1
		) tbl1
	group by 1
	) tbl4 using(report_date)
order by 1; 
	