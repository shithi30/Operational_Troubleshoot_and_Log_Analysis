/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): This user got 21 inbox messages on yesterday? Could you please investigate the issue that what actually happened! 01980001603, 01980001564 (Awlad Bhai)
*/

select tbl1.*, tbl2.*
from 
	/*(select mobile_no, event_date schedule_date, event_name, notification_id, bulk_notification_id  
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date=current_date-1
		and event_name in('inbox_message_received')
		and mobile_no in('01980001564', '01980001603')
	) tbl1*/

	(select tallykhata_user_id, date(created_at) schedule_date, event_name, notification_id, bulk_notification_id 
	from public.sync_appevent
	where 
		date(created_at)=current_date-1
		and event_name in('inbox_message_received')
		and tallykhata_user_id in
			(select tallykhata_user_id
			from public.register_usermobile 
			where mobile_number in('01980001564', '01980001603')
			)
	) tbl1
	
	left join 
	
	data_vajapora.all_sch_stats tbl2 using(schedule_date, bulk_notification_id);