{{ config(
    materialized='table',
    description='Table des customers par pays et notion de statut de client et profil beauté',
    persist_docs={"relation": true, "columns": true},
    on_schema_change='ignore' 

) }}

SELECT * FROM `normalised-417010.box.sku_by_user_by_box`
where box_id >100