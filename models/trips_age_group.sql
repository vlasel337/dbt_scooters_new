select
    t2."group" as age_group,
    t2.age_start || '-' || t2.age_end as age_range,
    count(*) as trips,
    sum(price_rub) as revenue_rub
from {{ ref("trips_users") }} as t1
cross join {{ ref("age_groups") }} as t2
where 1=1
    and t1.age >= t2.age_start
    and t1.age <= t2.age_end
group by 
    t2."group",
    t2.age_start || '-' || t2.age_end
order by
    t2.age_start || '-' || t2.age_end