WITH car_models AS (
    SELECT unnest(ARRAY[
        '쏘나타', '아반떼', '그랜저', '아이오닉', '캐스퍼', '포터', '로나',
        '넥쏘', '팰리세이드', '투싼', '베뉴', '싼타페', '스타리아', '제네시스'
    ]) AS car_model
),
accidents AS (
    SELECT unnest(ARRAY['급발진', '화재', '사고']) AS accident
) 
INSERT INTO accumulated_table (car_model, accident, accumulated_count, contents, is_alert, is_issue )
SELECT 
    c.car_model, 
    acc.accident, 
    0,                         
    jsonb_build_object('contents', '[]'::jsonb),
    false,
    false
FROM car_models c
CROSS JOIN accidents acc;