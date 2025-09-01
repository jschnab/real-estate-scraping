select
    zipcode
    , sum(case when beds = 1 then 1 else 0 end) beds_1
    , sum(case when beds = 2 then 1 else 0 end) beds_2
    , sum(case when beds = 3 then 1 else 0 end) beds_3
    , sum(case when beds = 4 then 1 else 0 end) beds_4
    , sum(case when beds >= 5 then 1 else 0 end) beds_5_more
from properties
group by 1
order by 2, 3, 4, 5, 6
;
