-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
  ORDER BY namefirst, namelast
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  HAVING avg(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT p.namefirst, p.namelast, p.playerid, h.yearid
  FROM people as p, halloffame as h
  ON p.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY h.yearid DESC, p.playerid
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT p.namefirst, p.namelast, p.playerid, c.schoolid, h.yearid
  FROM people as p
  INNER JOIN collegeplaying as c
  ON p.playerid = c.playerid AND c.schoolid IN (SELECT schoolid FROM schools WHERE schoolstate = 'CA')
  INNER JOIN halloffame as h
  ON p.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY yearid DESC, schoolid, p.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT p.playerid, p.namefirst, p.namelast, c.schoolid
  FROM people as p
  LEFT JOIN collegeplaying as c
  ON p.playerid = c.playerid
  INNER JOIN halloffame as h
  ON p.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY p.playerid DESC, schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, b.yearid, (CAST(b.h AS FLOAT) + CAST(b.h2b AS FLOAT) + 2*CAST(b.h3b AS FLOAT) +  3*CAST(b.hr AS FLOAT)) / CAST(b.ab AS FLOAT) as slg
  FROM people as p
  INNER JOIN batting as b
  ON p.playerid = b.playerid
  WHERE b.ab > 50
  ORDER BY slg DESC, b.yearid, p.playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, (SUM(b.h) + SUM(b.h2b) + 2*SUM(b.h3b) +  3*SUM(b.hr) +0.0) / (SUM(b.ab)+ 0.0 ) as lslg
  FROM people as p
  INNER JOIN batting as b
  ON p.playerid = b.playerid
  GROUP BY p.playerid
  HAVING SUM(b.ab) > 50
  ORDER BY lslg DESC, p.playerid
  LIMIT 10
;

CREATE VIEW lslg(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, (SUM(b.h) + SUM(b.h2b) + 2*SUM(b.h3b) +  3*SUM(b.hr) +0.0) / (SUM(b.ab)+ 0.0 ) as lslg
  FROM people as p
  INNER JOIN batting as b
  ON p.playerid = b.playerid
  GROUP BY p.playerid
  HAVING SUM(b.ab) > 50
  ORDER BY lslg DESC, p.playerid
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT namefirst, namelast, l.lslg
  FROM lslg as l
  WHERE l.lslg > (SELECT lslg FROM lslg WHERE playerid = 'mayswi01')
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary)
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT binid, 507500.0+binid*3249250,3756750.0+binid*3249250, count(*)
  FROM binids,salaries
  WHERE (salary BETWEEN 507500.0+binid*3249250 AND 3756750.0+binid*3249250 )AND yearID='2016'
  GROUP BY binid
;

CREATE VIEW salary_statistics(yearid, minsa, maxsa, avgsa)
AS 
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary)
  FROM salaries
  GROUP BY yearid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT s1.yearid, s1.minsa - s2.minsa, s1.maxsa - s2.maxsa, s1.avgsa - s2.avgsa
  FROM salary_statistics as s1
  INNER JOIN salary_statistics as s2
  ON s1.yearid - 1 = s2.yearid
  ORDER BY s1.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT p.playerid, p.namefirst, p.namelast, salary, yearid
  FROM salaries 
  INNER JOIN people as p
  ON salaries.playerid = p.playerid
  WHERE (yearid = 2000 AND salary = 
          (SELECT MAX(salary)
          FROM salaries as s1
          WHERE s1.yearid = 2000)
          )
          OR 
          (yearid = 2001 AND salary =
          (SELECT MAX(salary)
          FROM salaries as s2
          WHERE s2.yearid = 2001)
          )

;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamid, MAX(s.salary) - MIN(s.salary)
  FROM allstarfull as a
  INNER JOIN salaries as s 
  ON a.playerid = s.playerid AND a.yearid = s.yearid
  WHERE s.yearid = 2016
  GROUP BY a.teamid
;

