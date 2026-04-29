select *
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions
) AS row_num
FROM layoffs_staging;

WITH cte AS (
  SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions
  ) AS row_num
  FROM layoffs_staging
)
DELETE FROM cte
WHERE row_num > 1;

create table layoffs_2
like layoffs;


select *
from layoffs_2;

insert layoffs_2
select *
from layoffs;

SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions
) AS row_num
FROM layoffs_2;

WITH cte_2 AS (
  SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions
  ) AS row_num
  FROM layoffs_staging
)
DELETE FROM cte_2
WHERE row_num > 1;




CREATE TABLE `layoffs_3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_3;

insert into layoffs_3
SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions
) AS row_num
FROM layoffs_2;

DELETE FROM layoffs_3
WHERE row_num > 1;


-- STANDARDIZING DATA

select company,trim(company)
from layoffs_3;

update layoffs_3
set company=trim(company);

select * 
from layoffs_3
where industry like 'crypto%';

update layoffs_3
set industry = 'Crypto'
where industry like 'crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_3
ORDER BY 1;

UPDATE layoffs_3
SET country = TRIM(TRAILING ' ' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_3;

update layoffs_3
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_3;

alter table layoffs_3
modify column `date` date ;

-- removing null  or blank rows

select * 
from layoffs_3
where total_laid_off is null
and percentage_laid_off is null;

select distinct industry
from layoffs_3;

select * 
from layoffs_3
where industry is null
or industry ='';

select *
from layoffs_3
where company= 'Airbnb';

SELECT *
FROM layoffs_3 t1
JOIN layoffs_3 t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_3 t1
JOIN layoffs_3 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

update layoffs_3
set industry = null
where industry ='';


UPDATE layoffs_3 t1
JOIN layoffs_3 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

select * 
from layoffs_3
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_3
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_3;

alter table layoffs_3
drop column row_num;

-- Exploratory data analysis 

select *
from layoffs_3;

-- To find maximum layoff and percentage of maximum layoff( only one company could get that , so we are getting one value)
 
select max(total_laid_off),max(percentage_laid_off)
from layoffs_3;

-- to find companies that has 100% layoff ( we are using desc to find top 3 companies with highest layoff)

select *
from layoffs_3
where percentage_laid_off=1
order by total_laid_off desc; 

-- to find top companies that raised maximum funds in millions 

select *
from layoffs_3
where percentage_laid_off=1
order by funds_raised_millions desc; 

-- to find total or sum of laid off by a company 

select company,sum(total_laid_off)
from layoffs_3
group by company
order by 2 desc;

-- to find the layoff period from when to when 

select min(`date`),max(`date`)
from layoffs_3;

-- to find which industry hit the most layoffs

select industry,sum(total_laid_off)
from layoffs_3
group by industry 
order by 2 desc;

-- to find which country hit the most layoffs

select country,sum(total_laid_off)
from layoffs_3
group by country
order by 2 desc;

-- to find the amount of layoffs in a particular date 

select `date`,sum(total_laid_off)
from layoffs_3
group by `date`
order by 1 desc;

-- to find the layoffs by year

select year(`date`),sum(total_laid_off)
from layoffs_3
group by year(`date`)
order by 2 desc;

-- finding top layoffs using company's stages

select stage,sum(total_laid_off)
from layoffs_3
group by stage
order by 2 desc;

-- to find layoffs by date(another format)

select substring(`date`,1,7) as `MONTH`,sum(total_laid_off)
from layoffs_3
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 asc ;

select *
from layoffs_3;

-- using cte 

with rolling_total as 
(select substring(`date`,1,7) as `MONTH`,sum(total_laid_off) as total_off
from layoffs_3
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 asc )
select `MONTH`,total_off,sum(total_off) over(order by `MONTH` ) as Rolling_Total
from rolling_total;

-- to find layoffs by both company and year

select company,year(`date`),sum(total_laid_off)
from layoffs_3
group by company,year(`date`)
order by company asc;

-- finding top 5 companies which laid off more in a particular year using cte 

with Company_Year (company, years,total_laid_off) as
(select company,year(`date`),sum(total_laid_off)
from layoffs_3
group by company,year(`date`)
order by company asc), Company_Year_Rank as
(select *, dense_rank() over(partition by years order by total_laid_off desc) as Ranking 
from Company_Year
where years is not null)
select *
from Company_Year_Rank
where Ranking <=5;





 