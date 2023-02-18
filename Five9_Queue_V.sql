CREATE OR REPLACE VIEW DEVELOPMENT.DW_EXTRACT.FIVE9_QUEUE_V AS 
/* 
This script aims to provide a comprehensive dataset for the queue level statistics such as abandons, queue wait time, speed of answer, and queue callbacks. 
The granularity is at the queue level. Note that since a given call may feature multiple queues, each queue is considered a "queue touch", not a call. 
A queue is eligible for inclusion if it's the initial queue a partner enters upon calling post IVR, or, if a technician cold transfers the call back into the IVR. 
Warm transfers, conferences, and consults are excluded from this dataset - as they are initiated by the technician, not the caller. 
*/

--this step creates a flag for Weekend Blue campaigns, a campaign with wider overflow eligibility and shorter queue timeout times. 
WITH Weekend_Blue AS 
(SELECT DISTINCT call_id FROM dw_extract.five9_call_segment_s cs WHERE called_party LIKE '%Weekend Blue%')

-- exposes a given segment's previous and subsequent segment types/skills. These are needed for classification later in the script.
,staging_agg AS 
(select
cs.call_id,
cs.call_segment_id,
cs.called_party,
cs.result,
cs.segment_type,
cs.disposition,
cs.session_id, 
cs.segment_time_s,
cs.queue_wait_time_s,
cs.campaign,
cs.timestamp,
CASE WHEN wb.call_id IS NOT NULL THEN 1 ELSE 0 END AS WEEKEND_BLUE_FLG,
lead(cs.segment_type::text, 1) OVER(PARTITION BY cs.session_id ORDER BY cs.timestamp DESC) AS prev_type, 
lead(cs.segment_type::text, 1) OVER(PARTITION BY cs.session_id ORDER BY cs.timestamp) AS next_type,
lead(cs.called_party::text, 1) OVER(PARTITION BY cs.session_id ORDER BY cs.timestamp) AS next_skill,
LAG(cs.called_party::text, 1) OVER(PARTITION BY cs.session_id ORDER BY cs.timestamp) AS prev_skill,
lead(cs.queue_callback_wait_time_s, 1) OVER(PARTITION BY cs.session_id ORDER BY cs.timestamp) AS queue_callback_wait_time_s
FROM dw_extract.five9_call_segment_s cs
LEFT JOIN WEEKEND_BLUE wb ON cs.call_id = wb.call_id)

-- this step:
-- isolates skill transfer segments.
-- filters out warm transfers, consults, and conferences as the technician initiates these, not the caller.
-- creates helper values used in the group_queues_agg (first_xxx and last_xxx) to account for the following scenario:
---- Five9 creates two segments when a queue times out and prompts the caller to leave a callback number. Technically, this should be one segment as the caller is in one queue for the duration of the scenario.
---- see call Id 300000000055804

,skill_transfer AS
(select 
 row_number() OVER(PARTITION BY session_id, segment_type ORDER BY sa.timestamp ASC) as skill_transfer_number,
FIRST_VALUE(CALL_SEGMENT_ID) OVER(Partition BY session_id, segment_type, called_party ORDER BY timestamp ASC) as first_call_segment_id,
 FIRST_VALUE(PREV_TYPE) OVER(Partition BY session_id, segment_type, called_party ORDER BY timestamp ASC) as first_prev_type,
 LAST_VALUE(NEXT_TYPE) OVER(Partition BY session_id, segment_type, called_party ORDER BY timestamp ASC) as last_next_type,
 FIRST_VALUE(PREV_SKILL) OVER(Partition BY session_id, segment_type, called_party ORDER BY timestamp ASC) as first_prev_skill,
 LAST_VALUE(NEXT_SKILL) OVER(Partition BY session_id, segment_type, called_party ORDER BY timestamp ASC) as last_next_skill,
 sa.*
 from staging_agg  sa
 WHERE segment_type = 'Skill Transfer'
AND (PREV_TYPE NOT LIKE '%Consult%' AND PREV_TYPE NOT LIKE '%Warm Transfer%' AND PREV_TYPE NOT LIKE '%Conference%' AND PREV_TYPE NOT LIKE '%Silent Monitor%')
AND CALLED_PARTY <> 'testdev'
AND CAMPAIGN <> 'inbound_dev')
 
-- this step normalizes the dataset to output one segment in the case when Five9 generates two segments if a queue times out. 
,group_queues AS
(select 
CALL_ID
,FIRST_CALL_SEGMENT_ID AS CALL_SEGMENT_ID
,CALLED_PARTY
,Result
,Disposition
,first_prev_type as PREV_TYPE
,last_next_type as NEXT_TYPE
,first_prev_skill AS PREV_SKILL
,last_next_skill AS NEXT_SKILL
,WEEKEND_BLUE_FLG
,MIN(SKILL_TRANSFER_NUMBER) AS SKILL_TRANSFER_NUMBER
,MIN(TIMESTAMP) AS TIMESTAMP
,SUM(QUEUE_WAIT_TIME_S) AS QUEUE_WAIT_TIME_S
,SUM(SEGMENT_TIME_S) AS SEGMENT_TIME_S
,SUM(QUEUE_CALLBACK_WAIT_TIME_S) AS QUEUE_CALLBACK_WAIT_TIME_S
FROM skill_transfer
GROUP BY 1,2,3,4,5,6,7,8,9,10
)

-- this step creates a more detailed classification for the outcome of the call (NEW_RESULT)
,new_result_agg AS
(SELECT 
timestamp,
 CALL_SEGMENT_ID,
WEEKEND_BLUE_FLG,
call_id,
called_party as skill,
result,
next_type,
prev_type,
prev_skill,
next_skill, 
disposition,
segment_time_s,
queue_wait_time_s,
CASE WHEN queue_callback_wait_time_s >0 THEN queue_callback_wait_time_s ELSE NULL END as queue_callback_wait_time_s,
1 as queue_touch,
CASE WHEN SKILL LIKE '%billing%' OR SKILL LIKE '%collections%'  THEN 'billing' ELSE 'support' END as department,
CASE WHEN queue_wait_time_s = 0 THEN 1 ELSE 0 END as excl_from_metrics, 
CASE WHEN Result = 'Answered' THEN 'Answered'
WHEN NEXT_TYPE = 'Queue Callback' THEN 'Queue Callback Returned'
WHEN DISPOSITION = 'Queue Callback Timeout' THEN 'Queue Callback Timeout'
WHEN Result = 'Queue Timeout' AND Next_Type IS NULL AND DISPOSITION NOT LIKE '%Queue Callback%' THEN 'Queue Timeout - Abandoned'
WHEN RESULT = 'Queue Timeout' and Next_Type = 'Skill Transfer' THEN 'Queue Timeout - Overflow'
WHEN RESULT = 'Queue Timeout' THEN 'Queue Timeout - Other'
WHEN Result = 'Disconnected' AND NEXT_TYPE IS NULL AND DISPOSITION NOT LIKE '%Queue Callback%' THEN  'Abandoned Disconnect'
END AS NEW_RESULT,
CASE WHEN skill_transfer_number = 1 THEN 1 ELSE 0 END as  is_initial_skill,
CASE WHEN PREV_TYPE = 'Transfer to IVR' THEN 1 ELSE 0 END as is_cold_transfer
FROM group_queues

),

-- this step translates the classifications in NEW_RESULT to metrics as appropriate
queue_prod_agg
AS
(SELECT 
TIMESTAMP
,CALL_SEGMENT_ID
,WEEKEND_BLUE_FLG
,CALL_ID
,SKILL
,QUEUE_WAIT_TIME_S
,NEW_RESULT
,IS_INITIAL_SKILL
,IS_COLD_TRANSFER
,DEPARTMENT
,QUEUE_CALLBACK_WAIT_TIME_s
,CASE WHEN NEW_RESULT = 'Answered' THEN QUEUE_WAIT_TIME_S END AS SPEED_OF_ANSWER_S
,CASE WHEN NEW_RESULT LIKE '%Abandon%'THEN QUEUE_WAIT_TIME_S END AS TIME_TO_ABANDON_S
,EXCL_FROM_METRICS
,CASE WHEN NEW_RESULT = 'Answered' THEN 1 ELSE 0 END AS ANSWERED
,CASE WHEN NEW_RESULT = 'Abandoned Disconnect' THEN 1 ELSE 0 END AS ABANDONED_DISCONNECT
,CASE WHEN NEW_RESULT = 'Queue Timeout - Overflow' THEN 1 ELSE 0 END AS QUEUE_TIMEOUT_OVERFLOW
,CASE WHEN NEW_RESULT = 'Queue Timeout - Abandoned' THEN 1 ELSE 0 END AS ABANDONED_QUEUE_TIMEOUT
,CASE WHEN NEW_RESULT LIKE '%Abandon%' THEN 1 ELSE 0 END AS ABANDONED
,CASE WHEN NEW_RESULT = 'Queue Timeout - Other' THEN 1 ELSE 0 END AS QUEUE_TIMEOUT_OTHER
,CASE WHEN NEW_RESULT = 'Queue Callback Returned' THEN 1 ELSE 0 END AS QUEUE_CALLBACK_ANSWERED
,CASE WHEN NEW_RESULT = 'Queue Callback Timeout' THEN 1 ELSE 0 END AS QUEUE_CALLBACK_TIMEOUT
,CASE WHEN NEW_RESULT LIKE '%Queue Callback%' THEN 1 ELSE 0 END AS QUEUE_CALLBACKS 
,1 AS QUEUE_TOUCHES
FROM new_result_agg
)

select * FROM queue_prod_agg;