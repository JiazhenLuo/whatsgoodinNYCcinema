-- 删除"Showtimes coming soon"电影及其放映记录
DELETE FROM screenings WHERE movie_id IN (SELECT id FROM movies WHERE title_en LIKE '%Showtimes coming soon%');
DELETE FROM movies WHERE title_en LIKE '%Showtimes coming soon%';

-- 创建临时表来标记要保留的放映记录（每组中ID最小的）
CREATE TEMPORARY TABLE screening_to_keep AS
SELECT MIN(id) as id
FROM screenings
GROUP BY movie_id, date, time;

-- 删除所有不在保留列表中的放映记录
DELETE FROM screenings 
WHERE id NOT IN (SELECT id FROM screening_to_keep);

-- 清理临时表
DROP TABLE screening_to_keep;

-- 显示完成信息
SELECT 'Database cleanup completed' as Message; 