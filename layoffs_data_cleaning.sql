-- Create a staging table
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT INTO world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Remove duplicate rows using ROW_NUMBER
CREATE TABLE world_layoffs.layoffs_staging2 AS
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ORDER BY company
    ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Keep only first occurrence of duplicates
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Drop helper column
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- Standardize data: Replace empty strings with NULL in `industry`
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Fill NULL industries using same company name
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Normalize industry names
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Fix and convert `date` field
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Remove rows with no data in both `total_laid_off` and `percentage_laid_off`
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
