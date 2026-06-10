-- Answer these 5 business questions

-- 1. Which conservation zones have the highest calculated risk scores?
-- Tells managers exactly where to deploy heavy security today.
select any_value(z.zone_name) as conservation_zone, any_value(z.habitat_type) as type_of_habitat, any_value(p.risk_score) as percentage_risk
from `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_zone` as z
inner join `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_patrol` as p
on z.zone_id = p.zone_id 
group by z.zone_name
order by percentage_risk desc
;
-- 2. What is the most common type of incident involving high-risk animals (like Rhinos and Elephants)?
-- Helps anti-poaching units prepare the right equipment

select count(incident_id) as total_incidents, incident_type
  from `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_incidents` 
  
  GROUP BY incident_type
  ORDER BY total_incidents DESC
  limit 20;
-- 3. Do poaching and Trespassing incidents occur mostly which month and is it weekend?
-- 
select i.incident_type, d.month, d.is_weekend
from `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_date` as d
inner join `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_incidents` as i
on i.date_id = d.date_id
where i.incident_type in( "Trespassing" , "Poaching Attempt") 
order by d.month
;


-- 5. What percentage of poaching and trespassing incidents result in an arrest vs investigating ?
-- It proves to the park directors whether their anti-poaching strategies are actually working or if the poachers are winning
with cte as(
select incident_type, outcome, count(*)as count
from `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_incidents`
where incident_type in ("Poaching Attempt", "Trespassing") and
outcome in("Arrested", "Investigating")
group by outcome, incident_type
order by count
)

select incident_type, outcome, avg(count) as percentage
from cte
group by incident_type, outcome
;
