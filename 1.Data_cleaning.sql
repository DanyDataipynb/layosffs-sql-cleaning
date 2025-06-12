-- Data Cleaning / Limpieza de Datos 

SELECT * 
FROM layoffs;  
-- View the raw data / Ver los datos originales

-- 1. Remove Duplicates / Eliminar duplicados 
-- 2. Standardize the Data / Estandarizar los datos 
-- 3. Handle Null values / Manejar los campos vacíos 
-- 4. Remove unnecessary columns / Eliminar columnas si es necesario

-- Create a staging table to work on / Crear una tabla de staging para trabajar
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;  
-- Check the new empty staging table / Revisar que la tabla de staging esté vacía

-- Copy original data into staging table / Copiar los datos originales a la tabla de staging
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Create a row number to identify duplicates / Crear un número de fila para identificar duplicados
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Use CTE to detect duplicates / Usar CTE para detectar duplicados
WITH duplicate_cte AS 
(
  SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions) AS row_num
  FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;  
-- Show only duplicate rows / Mostrar solo las filas duplicadas

-- Verify a specific entry / Verificar una entrada específica
SELECT * 
FROM layoffs_staging
WHERE company = ' E Inc.';

-- Attempt to delete duplicates (this won't work in MySQL) / Intentar eliminar duplicados (esto no funcionará en MySQL)
WITH duplicate_cte AS 
(
  SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions) AS row_num
  FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1; 
-- ❗ Esto dará error en MySQL: no se puede eliminar directamente desde un CTE / This will throw an error in MySQL

-- Create a new table with an extra column for row numbers / Crear una nueva tabla con columna adicional para el número de fila
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;  
-- Check for any row_num > 1 just in case / Verificar filas con row_num > 1 por si acaso

-- Insert data into new staging table with row numbers / Insertar datos con número de fila
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER( 
  PARTITION BY company, location,
  industry, total_laid_off, percentage_laid_off, `date`, stage,
  country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Delete duplicates from new table / Eliminar duplicados de la nueva tabla
DELETE  
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *   
FROM layoffs_staging2;
-- View cleaned table / Ver tabla limpia

-- STANDARDIZING DATA / ESTANDARIZACIÓN DE DATOS

-- Check company names with extra spaces / Ver nombres de empresas con espacios extra
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

-- Fix extra spaces in company names / Corregir espacios en los nombres de empresa
UPDATE layoff_stading2
SET company = TRIM(company);
-- ❗ Hay un error tipográfico en el nombre de la tabla: "layoff_stading2" debería ser "layoffs_staging2"

-- Unify 'Crypto' categories / Unificar categorías de 'Crypto'
SELECT industry 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Clean up country names / Limpiar nombres de países
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1; 

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert date format / Convertir formato de fecha
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Check if dates were updated / Verificar si las fechas se actualizaron
SELECT `date`
FROM layoffs_staging2;

-- Change date column type / Cambiar tipo de dato de la columna de fecha
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Check rows with NULL values in key fields / Ver filas con valores nulos en campos clave
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Set empty strings in industry as NULL / Establecer cadenas vacías como NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Try to fill NULL values in industry based on matching rows / Llenar valores nulos usando datos duplicados con industry no nulo
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
  AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Update NULL industry values / Actualizar los valores nulos en industry
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Delete remaining rows with no layoff data / Eliminar filas restantes sin datos de despidos
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL  
AND percentage_laid_off IS NULL;

-- Final cleaned table / Tabla final ya limpiada
SELECT * 
FROM layoffs_staging2;

-- Remove row_num column / Eliminar la columna row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
