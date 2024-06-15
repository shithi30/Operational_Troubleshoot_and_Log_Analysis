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
	- Campaign Deletion in Live
	- IMPORTANT! Request for CJM Campaigns Hold/Delete!
- Notes (if any): 
*/

/*bulk TG*/

-- select *
delete
from public.notification_bulknotificationsendrequest
where request_id in(15357);

-- select *
delete
from public.notification_bulknotificationrequest
where id in(15357); 

/*auto TG*/

-- select *
delete
from public.notification_bulknotificationschedule
where request_id in(13492, 14009);

-- select *
delete
from public.django_celery_beat_periodictask
where name='f2a2af76-c204-4e94-bc1b-6c8250f24f6c';
