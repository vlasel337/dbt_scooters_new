with rides_by_company as (
    select 
        t2.company,
        count(*) as cnt_trips
    from {{ ref("trips_prep") }} t1
    left join {{ ref("scooters") }} t2
        on t1.scooter_hw_id = t2.hardware_id
    group by t2.company
)
select
    t1.company,
    t2.scooters,
    cast(t1.cnt_trips as float)/cast(t2.scooters as float) as trips_per_scooter
from rides_by_company t1
left join {{ ref("companies") }} t2
    on t1.company = t2.company
