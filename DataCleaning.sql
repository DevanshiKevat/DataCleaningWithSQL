-- Data Cleaning

select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null calue or blank values
-- 4. remove any row or column if necessary

create table layoffs_staging like layoffs; -- it will just make a table schema like layoffs
select * from layoffs_staging;

 -- inserting all the data of layoffs into layoffs_staging(copy the data to change it)
 insert layoffs_staging 
 select * from layoffs;
 
 -- Identifying duplicates
 with duplicate_cte as(
 select * , 
 row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) as row_num
 from layoffs_staging
)
select * from duplicate_cte
where row_num > 1;

select * from layoffs_staging 
where company = 'Cazoo';

-- //// Error Code: 1288. The target table duplicate_cte of the DELETE is not updatable	0.000 sec
-- with duplicate_cte as(
--  select * , 
--  row_number() over(partition by company, location, industry, total_laid_off, 
--  percentage_laid_off, `date`, stage, country, funds_raised) as row_num
--  from layoffs_staging
-- )
-- delete from duplicate_cte where row_num > 1;
 
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` Int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * from layoffs_staging2;

Insert into layoffs_staging2
select * , 
 row_number() over(partition by company, location, industry, total_laid_off, 
 percentage_laid_off, `date`, stage, country, funds_raised) as row_num
 from layoffs_staging
;

select * from layoffs_staging2
where row_num > 1;

Delete from layoffs_staging2
where row_num > 1;

-- Standardizing Data

Select company , trim(company) from layoffs_staging2;

Update layoffs_staging2
set company = Trim(company);


-- select * from layoffs_staging2 where industry like 'crypto%';

Select distinct(location) from layoffs_staging2 order by 1;

Select distinct(country) from layoffs_staging2 order by 1;

-- here all is set by if there are two country like US and United States then both are used for same country, 
-- so we have to make it same, we can do it using update statements

select distinct country from layoffs_staging2  where country like 'United%' ;

-- update layoffs_staging2 
-- set  country = 'United States'
-- where country like 'US%';

-- or if US and US., means just '.' is an issue than we can trim to that '.'

-- select distinct country, trim(trailing '.' from country)
-- from layoffs_staging2
-- order by 1;

-- update layoffs_staging2 
-- set  country = trim(trailing '.' from country)
-- where country like 'United states%';
select * from layoffs_staging2;

-- now looking to date, it is a type of text

select `date` ,
Str_to_date(`date`,'%Y-%m-%d')
from layoffs_staging2;

update layoffs_staging2
set `date` = Str_to_date(`date`,'%Y-%m-%d');

-- if i have same company and its location from which one's industry is mentioned and another is null then i can impute same industry name in empty row
-- first check  
select  t1.industry,  t2.industry from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null and t2.industry is not null;

-- impute industry

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;


alter table layoffs_staging2
modify column `date` DATE;
-- select `date` from layoffs_staging2; 

select * from layoffs_staging2;

select * from layoffs_staging2 where total_laid_off = '' and percentage_laid_off = '';

 Select distinct(industry) from layoffs_staging2;
 Select * from layoffs_staging2 where industry  = '' or industry = 'No' order by 1;

Select * from layoffs_staging2 where company = 'Appsmith';

update layoffs_staging2
set total_laid_off = null where total_laid_off = '';

update layoffs_staging2
set funds_raised = null where funds_raised = '';

update layoffs_staging2
set percentage_laid_off = null where percentage_laid_off = '';

select * from layoffs_staging2 where percentage_laid_off is null and total_laid_off is null; 

delete from layoffs_staging2 where percentage_laid_off is null and total_laid_off is null;

delete from layoffs_staging2 where industry is null;

alter table layoffs_staging2 drop column row_num;

-- Exploratory Data Analysis with SQL

Select max(total_laid_off), max(percentage_laid_off) from layoffs_staging2;
select * from layoffs_staging2 where percentage_laid_off = 1 order by funds_raised DESC;

select company, sum(total_laid_off) from layoffs_staging2 group by company order by 2 DESC;

select industry, sum(total_laid_off) from layoffs_staging2 group by industry order by 2 DESC;

select country, sum(total_laid_off) from layoffs_staging2 group by country order by 2 DESC;

select year(`date`), sum(total_laid_off) from layoffs_staging2 group by year(`date`) order by 1 DESC;

select min(`date`), max(`date`) from layoffs_staging2;

with rolling_sum_cte as(
select substring(`date`,1,7)  as `month`, sum(total_laid_off) as total_off  from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month` order by 1)
select `month`,total_off, sum(total_off) 
over(order by `month`) as rolling_total from rolling_sum_cte;

with company_year (company,year, total_laid_off) as
(select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as 
(select *, Dense_rank() over (partition by year order by total_laid_off desc) as ranking 
from company_year
where year is not null)
select * from company_year_rank where ranking <= 5; 