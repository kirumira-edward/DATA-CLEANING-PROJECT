use world_layoffs;

select * from layoffs;

-- remove duplicates
-- standardise the data
-- null and blank values
-- remove unnecessary columns and rows


-- removing duplicates
create table layoffs_staging;
 drop table  layoffs_staging;

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

-- removing duplicates
-- our real duplicates
 select * 
  from (
select * ,
row_number() over( partition by company, industry, percentage_laid_off, total_laid_off, 'date', stage, country,funds_raised_millions) as row_num
from layoffs_staging
)
duplicates where row_num > 1;

-- so here out target table is not deletable so i will just create another staging dataset with the row number column there then we delete it
with duplicates_cte as (
 select * 
  from (
select * ,
row_number() over( partition by company, industry, percentage_laid_off, total_laid_off, 'date', stage, country,funds_raised_millions) as row_num
from layoffs_staging
)
duplicates where row_num > 1
)
delete from  duplicates_cte;

CREATE TABLE layoffs_staging2 (
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
 -- we added a new column row number
INSERT INTO layoffs_staging2 (`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
select * from layoffs_staging2;

-- here we are deleting all the duplicates
-- but here you have to first untoggle the safe updates in the preferences section for this to run very well
DELETE  FROM layoffs_staging2
WHERE row_num > 1;



-- 2. STANDARDISE DATA( fixing issues in my data )
select * from layoffs_staging2;

-- industry has some empty rows
select distinct industry
from layoffs_staging2
order by 1;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- empty and blank rows
select distinct industry
from layoffs_staging2
where industry is null OR industry = ''
order by industry;

-- set the blank values to null
update layoffs_staging2
set industry = NULL
WHERE industry = '';

select distinct industry
from layoffs_staging2
where industry is null
order by industry;

-- lets populate the nulls
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company= t2.company
set t1.industry=t2.industry
where t1.industry is null and t2.industry is not null;

-- this shows us that it is only bally's without a populated orw for the null
select *
from layoffs_staging2
where industry is null OR industry = ''
order by industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- then also i see multiple cryptos in the industry so lets standardise that
select distinct industry
from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = 'Crypto'
where industry ='Crypto Currency';

update layoffs_staging2
set industry = 'Crypto'
where industry ='CryptoCurrency';

-- its now handled
select distinct industry
from layoffs_staging2
order by 1;

-- i also noticed that there are two united states ( United States and United States.) 
select distinct country
from layoffs_staging2
order by 1;

-- lets remove the full stop on the second Uniuted states
Update layoffs_staging2
set country = TRIM( Trailing '.' From country);

-- the final fixed issue
select distinct country
from layoffs_staging2
order by 1;

-- if you look at our data the datatype of date is text so we also need to chnage it
select * from layoffs_staging2;

-- lets use the str-to-date
update layoffs_staging2
SET `date`  =STR_TO_DATE(`date`, '%m/%d/%Y');

-- lets convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- confirmation
select * from layoffs_staging2;

-- 4. removing unnecessary columns

select * from layoffs_staging2
where total_laid_off is null;
-- unnecessary columns because without this information the data is useless to us
select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
-- delete them
delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- confirmation
select * from layoffs_staging2;

-- removing columns row_num
alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;


















































































