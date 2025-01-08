{{ config(
    database='normalized-417010',
    schema='user'
) }}

select count(*) as nb from teamdata-291012.user.customers 