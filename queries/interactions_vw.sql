create or replace view ea_insights_assessment.ea_insights_schema.interactions_vw as
with base as (
select
i.interaction_id
, i.customer_id
, i.agent_id
, i.channel
, i.queue
, to_timestamp(i.interaction_date_gmt, 'DD/MM/YYYY HH24:MI') as interaction_date_gmt
, i.interaction_duration
, i.post_call_handle_duration
, i.wait_time
, i.country
, i.resolved_ind

, a.employment_type
, a.tenure_months
, a.contact_centre
, a.completed_l1
, a.completed_l2
, a.completed_l3
, case when a.completed_l1 = true and a.completed_l2 = false and a.completed_l3 = false then true else false end as completed_l1_only
, case when a.completed_l2 = true and a.completed_l3 = false then true else false end as completed_l1l2
, case when a.completed_l3 = true then true else false end as completed_l1l2l3
from ea_insights_assessment.ea_insights_schema.interactions i
left join ea_insights_assessment.ea_insights_schema.agent_master_combined a on a.agent_id = i.agent_id
)

select *
from base

;

select *
from ea_insights_assessment.ea_insights_schema.interactions i
limit 10