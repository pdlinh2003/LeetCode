-- Game Play Analysis IV
with cte as(
select player_id, event_date
    , datediff(day, event_date, lead(event_date,1) over (partition by player_id order by event_date)) as diff
    , row_number() over (partition by player_id order by event_date) as row
from activity
)
select  convert(decimal(10,2),count(player_id) * 1.00/ (select count(distinct player_id) from activity)) as fraction
from cte 
where diff = 1 and row = 1

-- immediate food delivery II
with cte as(
select * 
    , row_number() over (partition by customer_id order by order_date) as rn
from delivery
)
select convert( decimal(10,2),count(customer_id) *100.00/ 
    (select count(customer_id) from cte where rn = 1)) as immediate_percentage
from cte
where rn =1
    and order_date = customer_pref_delivery_date


-- Second Highest Salary 
select max(salary) as SecondHighestSalary 
from employee
where salary != 
    (select max(salary) from employee)


-- investments in 2016
select convert(decimal(10,2),sum(tiv_2016)) as tiv_2016
from (
    select tiv_2015, tiv_2016
        , count(pid) over (partition by concat(lat,lon)) as latlon
        , count(pid) over (partition by tiv_2015) as tiv
    from insurance
) cte
where latlon = 1
    and tiv <> 1

-- Monthly transactions I
select format(trans_date, 'YYYY-MM') as month 
    , country
    , count(id) as trans_count
    , sum(case when state = 'approved' then 1 else 0 end) as approved_count
    , sum(amount) as trans_total_amount
    , sum(case when state = 'approved' then amount else 0 end) as approved_total_amount  
from Transactions
group by format(trans_date, 'YYYY-MM'), country


-- confirmation rate
select s.user_id
    , round(sum(case when action = 'confirmed' then 1 else 0 end)*1./count(*), 2) as confirmation_rate
from signups s
left join Confirmations c
on s.user_id = c.user_id
group by s.user_id


-- Managers with at least 5 direct reports
select name
from employee
where id in (
    select  managerid
    from employee
    group by managerid
    having count(managerid) >= 5) as cte

select name
from employee e
join (
    select managerId as id, count(*) as num
    from employee
    group by managerId
    having count(*) >=5
) cte
on e.id = cte.id


-- friend requests II: who has the most friends
select top 1 id
    , count(*) as num
from (
    select requester_id id
    from RequestAccepted
union all
    select accepter_id id
    from RequestAccepted
) as cte
group by id
order by num desc


-- product sales analysis III
Select product_id, year as first_year
    , quantity
    , price 
from Sales 
where concat(product_id,year) in (
        select concat(product_id, min(year))
        from sales 
        group by product_id
) 

-- customers who bought all products
select customer_id
from customer
group by customer_id
having count(distinct product_key) = (select count(*) from product)


-- consecutive numbers
with cte as(
    select *
        , lead(num,1) over (order by id asc) as num1
        , lead(num,2) over (order by id asc) as num2
    from logs
)
select distinct num ConsecutiveNums
from cte
where num=num1  
    and num=num2


-- product price at a given date
--c1
with cte as(
    select product_id
        , max(change_date) as date
    from products
    where change_date <= '2019-08-16'
    group by product_id
)
select product_id, 10 as price
from products
where product_id not in (select product_id from cte)
union 
select cte.product_id 
    , new_price as price
from cte
join products s
on cte.product_id = s.product_id 
    and cte.date = s.change_date


--c2
SELECT DISTINCT product_id
       ,COALESCE((SELECT TOP 1 new_price 
                  FROM products p2
                  WHERE p2.change_date <= '2019-08-16' AND p2.product_id = p1.product_id
                  ORDER BY change_date DESC), 10) AS price
FROM Products p1


-- last person to fit in the bus
select top 1 person_name 
from (
    select turn, person_name, weight
        , sum(weight) over (order by turn   
                        -- rows between unbounded preceding and current row 
                        ) as total
    from Queue 
) cte
where total <= 1000
order by total desc


-- count salary categories

with label as(
    select account_id 
        , case when income < 20000 then 'Low Salary'
            when income > 50000 then 'High Salary'
            else 'Average Salary'
            end category
    from accounts
)
select b.category
    , count(account_id) as accounts_count
from label l
right join (VALUES('Low Salary'),('High Salary'),('Average Salary')) AS b(category)
on l.category = b.category
group by b.category


-- exchange seat
select id
    ,case when id % 2 = 1 then lead(student,1,student) over (order by id)
        else lag(student,1) over (order by id)
        end student
from seat


-- movie rating
select * from (
    select top 1 name as results
    from movierating m
    left join users u
    on m.user_id = u.user_id
    group by name
    order by count(movie_id) desc, name
) topu
union all
select * from (
    select top 1 title as results
    from movierating mr
    left join movies m
    on mr.movie_id = m.movie_id
    where created_at like '2020-02%'
    group by title
    order by avg(rating*1.00) desc, title
) topm

-- restaurant growth
with cte as(
    select visited_on
        , sum(amount) amount
    from customer
    group by visited_on
)
, cte2 as(
    select visited_on
        , sum(amount) over (order by visited_on rows between 6 preceding and current row ) amount
        , avg(amount+0.0) over (order by visited_on rows between 6 preceding and current row ) average_amount
        , row_number() over (order by visited_on) rn
    from cte
)
select visited_on, amount, convert(decimal(10,2),average_amount) as average_amount
from cte2
where rn>=7


-- department top three salaries
with cte as(
    select e.* , d.name as Department
        , dense_rank() over (partition by departmentId order by salary desc) dr
    from employee e
    join department d
    ON e.departmentId = d.id
)
select Department, name as Employee, Salary
from cte
where dr <=3