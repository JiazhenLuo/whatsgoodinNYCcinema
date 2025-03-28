-- 添加常用查询的索引以提高性能

-- 为电影标题添加索引（用于搜索）
CREATE INDEX IF NOT EXISTS idx_movies_title_en ON movies(title_en);
CREATE INDEX IF NOT EXISTS idx_movies_title_cn ON movies(title_cn);

-- 为创建时间添加索引（用于最近添加的电影查询）
CREATE INDEX IF NOT EXISTS idx_movies_created_at ON movies(created_at);

-- 为TMDb和IMDb ID添加索引（用于外部API查询）
CREATE INDEX IF NOT EXISTS idx_movies_tmdb_id ON movies(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies(imdb_id);

-- 为放映信息添加索引
CREATE INDEX IF NOT EXISTS idx_screenings_movie_id ON screenings(movie_id);
CREATE INDEX IF NOT EXISTS idx_screenings_cinema ON screenings(cinema);
CREATE INDEX IF NOT EXISTS idx_screenings_date ON screenings(date);

-- ROLLBACK
-- DROP INDEX IF EXISTS idx_movies_title_en;
-- DROP INDEX IF EXISTS idx_movies_title_cn;
-- DROP INDEX IF EXISTS idx_movies_created_at;
-- DROP INDEX IF EXISTS idx_movies_tmdb_id;
-- DROP INDEX IF EXISTS idx_movies_imdb_id;
-- DROP INDEX IF EXISTS idx_screenings_movie_id;
-- DROP INDEX IF EXISTS idx_screenings_cinema;
-- DROP INDEX IF EXISTS idx_screenings_date; 