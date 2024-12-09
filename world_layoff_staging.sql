-- Data Cleaning
-- Dataset is downloaded from Kaggle 
-- (https://www.kaggle.com/datasets/swaptr/layoffs-2022).

-- importing the raw dataset by importing graphic wizard
CREATE DATABASE world_layoff;
USE world_layoff;
SELECT * FROM layoffs;


-- We will duplicate the table for the safety purpose.

CREATE TABLE layoffs_staging 
as 
SELECT * FROM layoffs;

-- Steps to follow in cleaning the data:
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- 1. check for duplicates and remove any

SELECT * FROM layoffs_staging;

-- using window function
SELECT *,
	ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, 
						percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num  
FROM layoffs_staging;


-- check for row_num = 2
-- we will use CTE to the above
WITH duplicate_values
as
(
SELECT *,
	ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, 
						percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num  
FROM layoffs_staging
)
SELECT * FROM duplicate_values
WHERE row_num > 1;  -- 5 rows returned as duplicates

-- Now delete the duplicates
CREATE TABLE layoffs_staging2
as
WITH duplicate_values
as
(
SELECT *,
	ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, 
						percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num  
FROM layoffs_staging
)
SELECT * FROM duplicate_values;

SELECT * 
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE row_num > 1; -- 5 rows duplicate deleted

-- now drop the extra column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;


-- 2. standardize data and fix errors
SELECT * 
FROM layoffs_staging2;
SELECT company FROM layoffs_staging2; -- white spaces are seen, removing white spaces

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT * FROM layoffs_staging2;


-- In industry we have crypto, cryto currency and cryptocurrency we have to correct them
SELECT DISTINCT industry FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- lets check country column
SELECT * FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';
-- we see that United States and United States. appears. we have to remove '.' from the 
-- country
SELECT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Next we see the date column, seems that datatype is text
SELECT * FROM layoffs_staging2;

SELECT `date`, str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER table layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Look at null values and see what 
SELECT * FROM layoffs_staging2;

select industry
from layoffs_staging2
where industry is null or industry = '';

update layoffs_staging2
set industry = null
where industry is null or industry = '';

select * from layoffs_staging2;

select company, location, industry
from layoffs_staging2
where industry is null;

select company, location, industry
from layoffs_staging2
where company like 'Airbnb%';
-- by seeing the output we can populate null values of industry to Travel for 
-- company Airbnb and others

select * from
layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE t1.industry is null 
and t2.industry is not null;

select t1.industry, t2.industry from
layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE t1.industry is null 
and t2.industry is not null;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT company, industry from layoffs_staging2
where industry is null;

-- 4. remove any columns and rows that are not necessary - few ways

SELECT * FROM layoffs_staging2;

-- check for null values in both column total_laid_off, percentage_laid_off
SELECT total_laid_off, percentage_laid_off
FROM layoffs_staging2
WHERE total_laid_off is null 
AND percentage_laid_off is null;

-- delete the rows having both columns null values (corres. to it)
DELETE FROM layoffs_staging2
WHERE total_laid_off is null 
AND percentage_laid_off is null;

-- removing column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;



















