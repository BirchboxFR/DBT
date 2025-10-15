{{ config(
    materialized='view',
    description='Table des customers par pays et notion de statut de client et profil beauté',
    persist_docs={"relation": true, "columns": true},
    on_schema_change='ignore' 

) }}

SELECT * FROM `teamdata-291012.user.Choose_by_user` 
where user_key='FR_2622634'