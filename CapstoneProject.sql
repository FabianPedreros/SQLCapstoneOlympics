-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Capstone Project
-- MAGIC ## Analyzing the Olympics Dataset 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Understanding the data

-- COMMAND ----------

SELECT * FROM noc_regions

-- COMMAND ----------

DESCRIBE noc_regions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the noc_regions i will use the columns: NOC and region.

-- COMMAND ----------

SELECT * FROM athlete_events

-- COMMAND ----------

DESCRIBE athlete_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the 'athlete_events' is ok to:
-- MAGIC 
-- MAGIC - In the Medal attribute is normal to have null values. This are the values with 'NA' 
-- MAGIC - Have multiple rows for the same ID and Name.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Cleansing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC In the athletes_events table i need to execute the next validation:
-- MAGIC 
-- MAGIC **For categorical attributes:**
-- MAGIC 
-- MAGIC 1. Validate null values in Name, Sex, Team, Noc, Games, Season, City, Sport, Event. 
-- MAGIC 2. Establish the step to follow with the null values found.
-- MAGIC 3. Validate the values in the next columns: Sex, Team, NOC, Games, Season, City, Sport, Event and Medal.
-- MAGIC 4. Establish the step to follow with the values that seems incorrect.
-- MAGIC 
-- MAGIC **For numeric attributes:**
-- MAGIC 
-- MAGIC 1. Validate null values in ID, Age, Height, Weight, and Year.
-- MAGIC 2. Establish the step to follow with the null values found.
-- MAGIC 3. Establish the min, max and range for the numeric values.
-- MAGIC 4. Establish the step to follow with the values that seems incorrect.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Categorical attributes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Validate null values in Name, Sex, Team, Noc, Games, Season, City, Sport, Event.

-- COMMAND ----------

-- Searching for rows with null name
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Name IS NULL

-- COMMAND ----------

-- Searching for rows with null team
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Team IS NULL

-- COMMAND ----------

-- Searching for rows with null sex
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Medal IS NULL

-- COMMAND ----------

-- Searching for rows with null NOC
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE NOC IS NULL

-- COMMAND ----------

-- Searching for rows with null games
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Games IS NULL

-- COMMAND ----------

-- Searching for rows with null season
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Season IS NULL

-- COMMAND ----------

-- Searching for rows with null city
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE City IS NULL

-- COMMAND ----------

-- Searching for rows with null sport
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Sport IS NULL

-- COMMAND ----------

-- Searching for rows with null event
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Event IS NULL

-- COMMAND ----------

-- Searching for rows with null Medal
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Medal LIKE 'NA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC - We have null values in the Medal column that have the 'NA' value, this have to be converted to the Null accepted for SQL. But not is possible for Databricks constraints. So the 'NA' Values is the Null value.
-- MAGIC - Due we dont have Null values it is needed to validate if the values are treated as 'NA', like we see in the case of Medals.

-- COMMAND ----------

-- Searching for rows with 'NA' Values
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Sport LIKE 'NA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After check in all the categorical columns for null values marked as 'NA', i don't find any.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.Establish the step to follow with the null values found.
-- MAGIC 
-- MAGIC - Only are found Null values in the Medal column, this are 231333 rows, not are an error, due in the data set we have all the history of the participations and are athletes that doesn't win a medal.
-- MAGIC - This values are represented with 'NA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.Validate the values in the next columns: Sex, Team, NOC, Games, Season, City, Sport, Event and Medal.

-- COMMAND ----------

-- Validate the values and quantity of rows by sex
SELECT 
  DISTINCT(Sex),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Sex

-- COMMAND ----------

-- Validate the values and quantity of rows by team
SELECT 
  DISTINCT(Team),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Team
ORDER BY Team

-- COMMAND ----------

-- Get the total of teams  values
SELECT 
  COUNT(DISTINCT(Team)) AS Quantity
FROM athlete_events

-- COMMAND ----------

-- Validate the values and quantity of rows by NOC
SELECT 
  DISTINCT(NOC),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY NOC
ORDER BY NOC

-- COMMAND ----------

-- Get the total of NOC  values
SELECT 
  COUNT(DISTINCT(NOC)) AS Quantity
FROM athlete_events

-- COMMAND ----------

-- Validate the values and quantity of rows by games
SELECT 
  DISTINCT(Games),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Games
ORDER BY Games

-- COMMAND ----------

-- Get the total of Games  values
SELECT 
  COUNT(DISTINCT(Games)) AS Quantity
FROM athlete_events

-- COMMAND ----------

-- Validate the values and quantity of rows by season
SELECT 
  DISTINCT(Season),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Season
ORDER BY Season

-- COMMAND ----------

-- Validate the values and quantity of rows by city
SELECT 
  DISTINCT(City),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY City
ORDER BY City

-- COMMAND ----------

-- Get the total of City  values
SELECT 
  COUNT(DISTINCT(City)) AS Quantity
FROM athlete_events

-- COMMAND ----------

-- Validate the values and quantity of rows by sport
SELECT 
  DISTINCT(Sport),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Sport
ORDER BY Sport

-- COMMAND ----------

-- Get the total of sports  values
SELECT 
  COUNT(DISTINCT(Sport)) AS Quantity
FROM athlete_events

-- COMMAND ----------

-- Validate the values and quantity of rows by event
SELECT 
  DISTINCT(Event),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Event
ORDER BY Event

-- COMMAND ----------

-- Get the total of events values
SELECT 
  COUNT(DISTINCT(Event)) AS Quantity
FROM athlete_events

-- COMMAND ----------

-- Validate the values and quantity of rows by medal
SELECT 
  DISTINCT(Medal),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Medal
ORDER BY Medal

-- COMMAND ----------

-- Get the total of medals values
SELECT 
  COUNT(DISTINCT(Medal)) AS Quantity
FROM athlete_events
  WHERE Medal <> 'NA'

-- COMMAND ----------

SELECT * FROM athlete_events limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.Establish the step to follow with the values that seems incorrect.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I don't find any value that seems incorrect and needs an update:
-- MAGIC 
-- MAGIC - We have two values for sex (M, F)
-- MAGIC - We have a large quantity of teams with 1184 
-- MAGIC - It appears we don't have any inconvenient with the NOC
-- MAGIC - Our data is from 51 different games
-- MAGIC - The seasons are divided in summer or winter
-- MAGIC - 42 different cities has been received the Olympics
-- MAGIC - 66 different sports are been included in the Olympics, but this is a global aggrupation like a sports category
-- MAGIC - 756 sports or events has been taken place in the Olympics
-- MAGIC - The medal earned has been Gold, Silver and Bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Numerical attributes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Validate null values in ID, Age, Height, Weight, and Year.
-- MAGIC 2. Establish the step to follow with the null values found.
-- MAGIC 3. Establish the min, max and range for the numeric values.
-- MAGIC 4. Establish the step to follow with the values that seems incorrect.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.Validate null values in ID, Age, Height, Weight, and Year.

-- COMMAND ----------

-- Searching for rows with null ID
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE ID IS NULL

-- COMMAND ----------

-- Searching for rows with null age
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Age IS NULL

-- COMMAND ----------

-- Searching for rows with null height
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Height IS NULL

-- COMMAND ----------

-- Searching for rows with null weight
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Weight IS NULL

-- COMMAND ----------

-- Searching for rows with null year
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Year IS NULL

-- COMMAND ----------

-- Searching for rows with NA age
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Age LIKE 'NA'

-- COMMAND ----------

-- Searching for rows with NA height
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Height LIKE 'NA'

-- COMMAND ----------

-- Searching for rows with NA weight
SELECT COUNT(*) AS Null_quantity 
FROM  athlete_events 
WHERE Weight LIKE 'NA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.Establish the step to follow with the null values found.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - We don't have the age for 9474 rows.
-- MAGIC - We don't have the height for 60171 rows.
-- MAGIC - We don't have the weight for 62875 rows.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.Establish the min, max and range for the numeric values.

-- COMMAND ----------

-- Establish the min, max and range for Age
SELECT
  MIN(CAST(Age AS INT)) AS Min_age,
  MAX(CAST(Age AS INT)) AS Max_age,
  MAX(CAST(Age AS INT)) - MIN(Age) AS Range_age
FROM  athlete_events
--WHERE Age <> 'NA'
WHERE Age IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The youngest person that participate in the Olympics had 10 years and the oldest had 97, i don't see a ilogicall value.

-- COMMAND ----------

-- Establish the min, max and range for height
SELECT
  MIN(CAST(Height AS INT)) AS Min_Height,
  MAX(CAST(Height AS INT)) AS Max_Height,
  MAX(CAST(Height AS INT)) - MIN(CAST(Height AS INT)) AS Range_Height
FROM  athlete_events
--WHERE Height <> 'NA'
WHERE Height IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The height is given in centimeters, the person with the shortest height in participating was 1.27 cm and the tallest was 2.26, there does not seem to be an extreme point to be verified or addressed. 

-- COMMAND ----------

-- Establish the min, max and range for weight
SELECT
  MIN(CAST(Weight AS INT)) AS Min_Weight,
  MAX(CAST(Weight AS INT)) AS Max_Weight,
  MAX(CAST(Weight AS INT)) - MIN(CAST(Weight AS INT)) AS Range_Weight
FROM  athlete_events
--WHERE Weight <> 'NA'
WHERE Weight IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Los pesos parecen estar dados en kilogramos, se observa un peso minimo bastante bajo, se podría llegar a validar este valor. 

-- COMMAND ----------

-- Establish the min, max and range for year
SELECT
  MIN(CAST(Year AS INT)) AS Min_Year,
  MAX(CAST(Year AS INT)) AS Max_Year,
  MAX(CAST(Year AS INT)) - MIN(CAST(Year AS INT)) AS Range_Year
FROM  athlete_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Exploration

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Univariate analysis for categorical attributes

-- COMMAND ----------

-- Quantity of events by sex
SELECT 
  DISTINCT(Sex),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Sex


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Here we can see how most of the events have been carried out by men, with 73% of participations compared to only 27% of women. 

-- COMMAND ----------

-- Quantity of persons by sex
SELECT 
  DISTINCT(Sex),
  COUNT(DISTINCT (ID)) AS Quantity
FROM athlete_events
GROUP BY Sex

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Talking about the athletes who have participated in the Olympics, we can see how men have represented 75% of the total number of participants and only 25% for women.

-- COMMAND ----------

-- Top ten teams with most participations
SELECT 
  DISTINCT(Team),
  COUNT(*) AS Quantity
FROM athlete_events
GROUP BY Team
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC United States is the team with most participation in the Olympics, followed by France and Great Britain.

-- COMMAND ----------

-- Top ten countries with more participations in the Olympics
SELECT 
  DISTINCT(region),
  COUNT(*) AS Quantity
FROM athlete_events a INNER JOIN noc_regions n
ON a.NOC = n.NOC
GROUP BY region
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When we look at the number of competitions per country, we can see that the USA is the country with the highest number of competitions, followed by Germany and France in third place. 

-- COMMAND ----------

-- Top ten games with more participations in the Olympics
SELECT 
  Games,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Games
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Historically, the largest number of events was held at the 2000 Summer Games, followed closely by the 1996 Summer Games. It seems that the summer games are the ones where the most games are held, since at least in the top ten there are no winter games.

-- COMMAND ----------

-- Top ten games with more distinct participant in the Olympics
SELECT 
  Games,
  COUNT(DISTINCT (Id)) AS Quantity
FROM athlete_events 
GROUP BY Games
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When reviewing the numbers of participants per game, we can see how the highest number has been presented in the 2016 summer games, in which 11,179 athletes participated.

-- COMMAND ----------

-- Top ten years with more participations in the Olympics
SELECT 
  Year,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Year
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC However, the largest number of events was held in 1992, when a total of 16,413 Olympic events were held.

-- COMMAND ----------

-- Top ten years with more distinct participant in the Olympics
SELECT 
  Year,
  COUNT(DISTINCT (Id)) AS Quantity
FROM athlete_events 
GROUP BY Year
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1992 is shown as the year in which the largest number of athletes participated with 11,183, surpassing by only four competitors those presented in 2016.

-- COMMAND ----------

-- Percentage of events per season
SELECT 
  Season,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Season
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The pie chart shows how most of the events have taken place in summer with 82 percent of the total.

-- COMMAND ----------

-- Top ten cities with more events in the Olympics
SELECT 
  City,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY City
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The largest number of sporting participations in the Olympics have taken place in London, followed by Athens and Sydney. The city of London has seen 22,426 participations take place.

-- COMMAND ----------

-- Top ten cities that have hosted the most athletes at the Olympics
SELECT 
  City,
  COUNT(DISTINCT(Id)) AS Quantity
FROM athlete_events 
GROUP BY City
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Throughout the history of the Olympics, London has had by far the largest total number of athletes, hosting a total of 18,941 athletes.

-- COMMAND ----------

-- Top ten sports with most participations
SELECT 
  Sport,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Sport
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Athletics is the king sport, here we show how the majority of participations have been in this sport, followed by gymnastics and swimming.

-- COMMAND ----------

-- Top ten sports by athletes participation
SELECT 
  Sport,
  COUNT(DISTINCT(Id)) AS Quantity
FROM athlete_events 
GROUP BY Sport
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Athletics is by far the sport in which the largest number of athletes participate with a historical total of 20,071 people.

-- COMMAND ----------

-- Top ten events with most participations
SELECT 
  Event,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Event
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC However, when reviewing the data at the event level, we can see that the one with the highest number of participations is in men's football. And since the first five events belong to team sports, this is logical since a greater number of people participate than in individual events. Men's individual gymnastics is the event with the highest number of individual participants.

-- COMMAND ----------

-- Top ten events by athletes participation
SELECT 
  Event,
  COUNT(DISTINCT(Id)) AS Quantity
FROM athlete_events 
GROUP BY Event
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When we look at the data by individual participation, it reflects what we have seen above, and that is that group sports have a higher participation in the Olympics.

-- COMMAND ----------

-- Medals distribution
SELECT 
  Medal,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Medal
HAVING Medal <> 'NA'
ORDER BY Quantity DESC

LIMIT 10

-- COMMAND ----------

-- Top ten Atletes with more gold medals
SELECT 
  Name,
  Medal,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Name, Medal
HAVING Medal = 'Gold'
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- Top ten Atletes with more silver medals
SELECT 
  Name,
  Medal,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Name, Medal
HAVING Medal = 'Silver'
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- Top ten Atletes with more bronze medals
SELECT 
  Name,
  Medal,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Name, Medal
HAVING Medal = 'Bronze'
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- Top ten Atletes with more participations in events
SELECT 
  Name,
  COUNT(*) AS Quantity
FROM athlete_events 
GROUP BY Name
ORDER BY Quantity DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Univariate analysis for numerical attributes

-- COMMAND ----------

-- Age distribution
SELECT CAST (age AS INT)
FROM athlete_events
WHERE age IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We see a normal distribution right skewed for age, showing us how the mayor quantity of athletes are near 25 five years old or less.

-- COMMAND ----------

-- Height distribution
SELECT CAST (Height AS INT)
FROM athlete_events
WHERE Height IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the case of height we can see in the histogram a normal distribution with a mean and median relatively close to 180 cm.

-- COMMAND ----------

-- Weight distribution
SELECT CAST (Weight AS INT)
FROM athlete_events
WHERE Weight IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When reviewing the weight distribution we can notice that it has a normal distribution skewed to the right, but that most of the athletes weigh between 56 and 75 kg. Being very few cases that exceed 100 kg, but presenting extreme points that reach to exceed that limit.

-- COMMAND ----------

-- Weight distribution
SELECT CAST (Weight AS INT)
FROM athlete_events
WHERE Weight IS NOT NULL

-- COMMAND ----------

SELECT * FROM athlete_events
where Height is not null
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Analysis

-- COMMAND ----------

-- Create a view for the Athletics with the attributes need it

CREATE VIEW IF NOT EXISTS Athletes
AS
  SELECT 
    SEX,
    Age,
    Height,
    Weight,
    CASE WHEN Age > 20 THEN 0 WHEN Age < 21 THEN 1 END AS Under_21,
    CASE WHEN Medal <> 'NA' THEN 1 WHEN Medal = 'NA' THEN 0 END AS Medal_winner

  FROM athlete_events 
  WHERE (Sport = 'Athletics' AND Age IS NOT NULL) AND
   (Height IS NOT NULL AND Weight IS NOT NULL)
  

-- COMMAND ----------

-- Validate the view creation
select * from Athletes

-- COMMAND ----------

-- Winners rate by sex
SELECT  
  SEX,
  SUM(Medal_winner) Medal_winners,
  (COUNT(*) - SUM(Medal_winner)) No_medal_winners
FROM Athletes
GROUP BY SEX

-- COMMAND ----------

-- Winners percentage by sex
SELECT
SEX,
(Medal_winners * 100/Total_athletes) Winners_percentage
FROM(
SELECT  
  SEX,
  SUM(Medal_winner) Medal_winners,
  COUNT(*) Total_athletes
FROM Athletes
GROUP BY SEX) 

-- COMMAND ----------

-- Winners rate by younger or not
SELECT  
  Under_21,
  SUM(Medal_winner) Medal_winners,
  (COUNT(*) - SUM(Medal_winner)) No_medal_winners
FROM Athletes
GROUP BY Under_21

-- COMMAND ----------

-- Winners percentage by younger or not
SELECT
Under_21,
(Medal_winners * 100/Total_athletes) Winners_percentage
FROM(
SELECT  
  Under_21,
  SUM(Medal_winner) Medal_winners,
  COUNT(*) Total_athletes
FROM Athletes
GROUP BY Under_21)

-- COMMAND ----------

SELECT 
  Height,
  Medal_winner
FROM Athletes

-- COMMAND ----------

SELECT 
  Height,
  Medal_winner
FROM Athletes

-- COMMAND ----------

SELECT 
  Medal_winner,
  avg (Height) Mean
FROM Athletes
GROUP BY Medal_winner

-- COMMAND ----------

SELECT 
  SEX,
  Height,
  Medal_winner
FROM Athletes

-- COMMAND ----------

SELECT 
  Height,
  Medal_winner,
  SEX
FROM Athletes
WHERE SEX = 'M'

-- COMMAND ----------

SELECT 
  Height,
  Medal_winner,
  SEX
FROM Athletes
WHERE SEX = 'F'

-- COMMAND ----------

SELECT 
  SEX,
  Medal_winner,
  avg (Height) Mean
FROM Athletes
GROUP BY SEX, Medal_winner
ORDER BY SEX, Medal_winner

-- COMMAND ----------

SELECT 
  Weight,
  Medal_winner
FROM Athletes

-- COMMAND ----------

SELECT 
  Weight,
  Medal_winner
FROM Athletes

-- COMMAND ----------

SELECT 
  Medal_winner,
  avg (Weight) Mean
FROM Athletes
GROUP BY Medal_winner

-- COMMAND ----------

SELECT 
  SEX,
  Weight,
  Medal_winner
FROM Athletes

-- COMMAND ----------

SELECT 
  Weight,
  Medal_winner,
  SEX
FROM Athletes
WHERE SEX = 'M'

-- COMMAND ----------

SELECT 
  Weight,
  Medal_winner,
  SEX
FROM Athletes
WHERE SEX = 'F'

-- COMMAND ----------

SELECT 
  SEX,
  Medal_winner,
  avg (Weight) Mean
FROM Athletes
GROUP BY SEX, Medal_winner
ORDER BY SEX, Medal_winner

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Analysis 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### New variables creation

-- COMMAND ----------

-- Create a view for the Athletics with the attributes need it
-- Creation of a categorical variable for age called Age_group

CREATE VIEW IF NOT EXISTS Athletes
AS
  SELECT 
    SEX,
    Age,
    Height,
    Weight,
    CASE WHEN Age between 10 and 19 THEN '10 to 19' 
    WHEN Age between 20 and 29 THEN '20 to 29'
    WHEN Age between 30 and 39 THEN '30 to 39'
    WHEN Age between 40 and 49 THEN '40 to 49'
    WHEN Age between 50 and 59 THEN '50 to 59'
    WHEN Age between 60 and 69 THEN '60 to 69'
    WHEN Age between 70 and 79 THEN '70 to 79'
    WHEN Age between 80 and 89 THEN '80 to 89'
    WHEN Age between 90 and 99 THEN '90 to 99'
    END AS Age_group,
    CASE WHEN Medal <> 'NA' THEN 1 WHEN Medal = 'NA' THEN 0 END AS Medal_winner

  FROM athlete_events 
  WHERE (Sport = 'Athletics' AND Age IS NOT NULL) AND
   (Height IS NOT NULL AND Weight IS NOT NULL)



-- COMMAND ----------

SELECT * FROM Athletes LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = sql('''SELECT * FROM Athletes''').toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.head()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analysing correlations between variables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Correlation between variables
-- MAGIC plt.figure(figsize=(10,10))
-- MAGIC sns.heatmap(df.corr(), annot = True, cmap = 'Reds')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Variables correlation for sex
-- MAGIC df_m = df[df['SEX']=='M']
-- MAGIC plt.figure(figsize=(10,10))
-- MAGIC sns.heatmap(df_m.corr(), annot = True, cmap = 'Reds')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Variables correlation for sex
-- MAGIC df_f = df[df['SEX']=='F']
-- MAGIC plt.figure(figsize=(10,10))
-- MAGIC sns.heatmap(df_f.corr(), annot = True, cmap = 'Reds')
-- MAGIC plt.show()

-- COMMAND ----------

SELECT 
  Age_group,
  Medal_winner,
  Height
FROM Athletes
WHERE SEX = 'M'

-- COMMAND ----------

SELECT 
  Age_group,
  Medal_winner,
  Height
FROM Athletes
WHERE SEX = 'F'
