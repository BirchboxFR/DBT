{{ config(
    database='normalised-417010',
    schema='user'
) }}

select count(*) as nb from teamdata-291012.user.customers 