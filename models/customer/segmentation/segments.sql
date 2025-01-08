{{ config(
    database='normalized',
    schema='user'
) }}

select count(*) as nb from teamdata-291012.user.customers 