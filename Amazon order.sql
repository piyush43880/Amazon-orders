-- 1- write a query to find top 3 and bottom 3 products by sales in each region.
   
SELECT 
    *
FROM
    orders;
    with regions as(  select region , product_id , sum(sales) as sale 
      from orders
       group by region , product_id ),
        cte as(
        select *,
        rank() over(partition by region order by sale desc) as rn
        from regions),
        cte2 as( select * ,
         rank() over(partition by region order by sale asc) as rn1
         from regions)
          select c.*, c1.* from cte2 as c
          inner join  cte as c1
          on c.region=c1.region  and c.rn1=c1.rn
         where rn1<=3 and rn<=3 ;
  -- 2- Among all the sub categories..which sub category had highest month over month growth by sales in Jan 2020 compare to dec 2019.
       with cte as( select extract(year from order_date) as years ,
        extract(month from order_date) as months,
        sub_category , sum(sales) as sale 
         from orders
          group by extract(year from order_date) ,
          extract(month from order_date) , sub_category),
          cte1 as(
           select * ,
            lag(sale) over(partition by sub_category order by years,months ) as prev_mon
           from cte)
            select * , round((sale- prev_mon)*100 /prev_mon,2) as growth from cte1
             where years=2020 and months=1
              order by growth desc
               limit 1;
             
  - -- 4- write a query to print top 3 products in each category by year over year sales growth in year 2020 compare to 2019.           
  select * from orders;
   with cte as( select extract(year from order_date) as years,
   category, product_id , sum(sales) as  sale
    from orders
     where extract(year from order_date) in(2019,2020)
      group by extract(year from order_date), category , product_id),
      cte1 as(select *, 
        lag(sale) over(partition by category, product_id  order by years) as prev_year
       from cte ),
        cte2 as( select *, round((sale-prev_year)*100/prev_year,2) as growth_year ,
        rank() over(partition by category order by (sale-prev_year)/prev_year desc) as rn 
        from cte1  where years=2020)
         select * from cte2 
          where rn<=3 ;
          
          
          
   -- 4 -- write a query to print top 3 products in each category by year over year sales growth in year 2020.
   
   with cte as(
   select extract(year from order_date) as "year" , category , product_id , sum(sales) as sales
   from orders
   group by extract(year from order_date), category, product_id),
   
   cte1 as(
    select *,
    lag(sales) over(partition by category , product_id order by year) as prev_year
    from cte),
    final as(
     select *,round((sales-prev_year)*100/prev_year,2) as yoy_growth
     from cte1
     where year=2020)
     
     select * from(
     select *,
      row_number() over(partition by category order by yoy_growth desc) as rn
      from final)a
      where rn<=3;
      
      
      
      
  -- 5     -- write a sql to find top 3 products in each category by highest rolling 3 months total sales for Jan 2020
   
   with cte as(
   select category , product_id,
   extract(year from order_date) as"year", extract(month from order_date) as"month", sum(sales) as sales
   from orders
   group by category ,product_id, extract(year from order_date), extract(month from order_date)),
   
   cte1 as(
   select * ,
   sum(sales) over(partition by category , product_id  order by year, month rows between 2 preceding and  current row) as rolling_3_month
   from cte)
   
   select  *  from(
   select * ,
   row_number () over(partition by category order by rolling_3_month desc) as rn
   from cte1 
   where year=2020 and month=1)a
   where rn<=3;
   
   
   -- write a query to find cities where not even a single order was returned.

select o.city , count(r.order_id) as cnt
 from orders as o
 left join returns as r on
 o.order_id=r.order_id 
 group by o.city
 having count(r.order_id)=0;
 
 
-- write a query to find total number of products in each category.

select category , count(product_id) as total_no_product
from orders
group by category;

-- write a query to find top 5 sub categories in west region by total quantity sold

 select region, sub_category ,sum(quantity) as total_quantity_sold
 from orders
 where region="west"
 group by region,sub_category
 order by total_quantity_sold desc
 limit 5;


-- write a query to find total sales for each region and ship mode combination for orders in year 2020.

 select  extract(year from order_date) as "year" ,region , ship_mode, sum(sales) as total_sales
 from orders
 where extract(year from order_date)=2020
 group by extract(year from order_date) ,region , ship_mode
 order by region , ship_mode;
  
  