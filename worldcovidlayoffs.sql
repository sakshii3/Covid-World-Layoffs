CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT * FROM world_layoffs.layoffs_staging;

SELECT * FROM (SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) 
AS row_num FROM layoffs_staging) duplicates 
WHERE row_num > 1;



-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);
select * from layoffs_staging2;
INSERT INTO `layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num >= 2;

select * from layoffs_staging2 where row_num >=2;

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT * FROM layoffs_staging2
WHERE company LIKE 'airbnb%';


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
ORDER BY industry;

SELECT DISTINCT industry FROM layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT * FROM layoffs_staging2;

SELECT DISTINCT country FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country FROM layoffs_staging2
ORDER BY country;

SELECT * FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off) from layoffs_staging2;

select * from layoffs_staging2 where percentage_laid_off=1  order by funds_raised_millions desc;

select year(`date`), sum(total_laid_off) from layoffs_staging2 group by year(`date`) order by 1;
select company ,sum(total_laid_off) from layoffs_staging2 group by company order by 2 DESC;
	
SELECT MIN(`DATE`) , MAX(`DATE`) FROM layoffs_staging2;

select INDUSTRY ,sum(total_laid_off) from layoffs_staging2 group by INDUSTRY order by 2 DESC;

select COUNTRY ,sum(total_laid_off) from layoffs_staging2 group by COUNTRY order by 2 DESC;

select company ,sum(total_laid_off) from layoffs_staging2 group by company order by 2 DESC;

select STAGE ,sum(total_laid_off) from layoffs_staging2 group by STAGE order by 2 DESC;
SELECT substring(`DATE`,1,7) AS `INTERVAL` ,
sum(total_laid_off) FROM layoffs_staging2 
WHERE substring(`DATE`,1,7) IS NOT NULL 
GROUP BY `INTERVAL` ORDER BY `INTERVAL`;

WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL 
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, total_laid_off, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE;


select company ,year(`date`), sum(total_laid_off) 
from layoffs_staging2 
group by company 
order by 3 DESC;


WITH Company_Year  AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) 
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
  )
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years , total_laid_off DESC;


