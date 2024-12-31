SELECT co.dw_country_code,
co.comment_id, 
co.comment_parent, 
FORMAT_DATE('%Y-%m-%d',co.comment_date) as review_date,
ca.product_id,
p.parent_post_id,
ca.brand_full_name, 
ca.product_nice_name as product_name, 
co.rating, 
c.id AS user_id,
co.comment_title as review_title, 
co.comment_content as review_body,
post_status,

FROM {{ ref('comments') } co
LEFT JOIN {{ ref('products') }} p ON p.post_id = co.comment_post_id AND p.dw_country_code = co.dw_country_code
LEFT JOIN {{ ref('catalog') }} ca ON ca.product_id = p.id AND ca.dw_country_code = p.dw_country_code
LEFT JOIN {{ ref('users') } c ON c.user_email = co.comment_author_email AND c.dw_country_code = co.dw_country_code
left JOIN {{ ref('posts') }} post ON post.id = p.post_id and post.dw_country_code=p.dw_country_code
WHERE co.comment_approved = '1'
AND co.comment_author_email NOT LIKE '%blissim%'
AND co.comment_author_email NOT LIKE '%birchbox%'
