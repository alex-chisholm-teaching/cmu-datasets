# US Presidential Elections Database

This directory contains a normalized relational database of US Presidential Elections from 1789-2024, designed for educational purposes.

## Database Schema

### Tables and Relationships

1. **candidates.csv** (Tab-separated format)
   - Primary Key: `candidate_id` (UUID)
   - Contains biographical information about presidential candidates
   - Fields: candidate_id, name, date_of_birth, date_of_death, birth_city, birth_state
   - Note: Uses tab-separated format to handle location names with commas

2. **political_parties.csv**
   - Primary Key: `party_id`
   - Contains information about political parties throughout US history
   - Fields: party_id, party_name, party_abbreviation, founded_year, ideology

3. **elections.csv**
   - Primary Key: `election_id`
   - Contains information about each presidential election including population data
   - Fields: election_id, election_year, election_type, total_electoral_votes, voter_turnout_percent, us_population_estimate, us_population_voting_age

4. **election_results.csv**
   - Primary Key: `result_id`
   - Junction table linking candidates, parties, and elections with their results
   - Foreign Keys:
     - `election_id` → elections.election_id
     - `candidate_id` → candidates.candidate_id
     - `party_id` → political_parties.party_id
   - Fields: result_id, election_id, candidate_id, party_id, electoral_votes, popular_votes, result_type

## Sample Queries (for educational use)

### Basic Queries
```sql
-- Find all winners
SELECT c.name, e.election_year, er.electoral_votes
FROM candidates c
JOIN election_results er ON c.candidate_id = er.candidate_id
JOIN elections e ON er.election_id = e.election_id
WHERE er.result_type = 'winner';

-- Count elections by party
SELECT p.party_name, COUNT(*) as wins
FROM political_parties p
JOIN election_results er ON p.party_id = er.party_id
WHERE er.result_type = 'winner'
GROUP BY p.party_name
ORDER BY wins DESC;
```

### Advanced Queries
```sql
-- Election results with population context
SELECT e.election_year, c.name as winner, er.popular_votes, 
       e.us_population_estimate, e.us_population_voting_age,
       ROUND((er.popular_votes * 100.0 / e.us_population_voting_age), 2) as percent_of_voting_age_pop
FROM elections e
JOIN election_results er ON e.election_id = er.election_id
JOIN candidates c ON er.candidate_id = c.candidate_id
WHERE er.result_type = 'winner' AND er.popular_votes IS NOT NULL
ORDER BY e.election_year;

-- Candidates by birth state
SELECT birth_state, COUNT(*) as candidate_count
FROM candidates
WHERE birth_state IS NOT NULL AND birth_state != ''
GROUP BY birth_state
ORDER BY candidate_count DESC;
```

## Loading Data with Pandas

```python
import pandas as pd

# Load candidates (tab-separated)
candidates = pd.read_csv('candidates.csv', sep='\t')

# Load other tables (comma-separated)
parties = pd.read_csv('political_parties.csv')
elections = pd.read_csv('elections.csv')
results = pd.read_csv('election_results.csv')
```

## Data Sources and Notes

- Candidate biographical data sourced from historical records and encyclopedic sources
- Population estimates combine US Census data with historical interpolations for both total and voting-age populations
- Some early elections lack popular vote data (before 1824)
- Party affiliations reflect historical context and may differ from modern party names
- UUIDs used for candidate_id to demonstrate modern database practices
- Tab-separated format used for candidates.csv to handle location names containing commas

## Educational Objectives

This dataset is designed to teach:
- Database normalization (1NF, 2NF, 3NF)
- Primary and foreign key relationships
- Junction tables for many-to-many relationships
- Historical data management challenges
- SQL query writing across multiple tables
- Data integrity and referential constraints
- Different file formats (CSV vs TSV) and their use cases

## File Format
- candidates.csv: Tab-separated values (TSV) with headers
- All other files: Comma-separated values (CSV) with headers
- All files suitable for import into any SQL database system (PostgreSQL, MySQL, SQLite, etc.)

## Assignment Questions

1. Load the database and store it locally. Make a connection using duckdb and verify that the connection was successfully made.

2. How many tables are available? Determine which tables could be joined by reporting back a list. Each potential list should have two tables and the name of the variable(s) that could be used to make a successful join. Note: The list itself does not need to be created programmatically. It can be compiled by using duckdb sql commands to explore what's available.

3. Create a table that shows...

## Analysis Ideas

- Count how many US presidents have come from each US state
- Which US political party has won the most elections? 
- What is the longest winning streak for any specific political party?
- Make a bar chart showing the number of elections won by each party
- What was the first election year for which there is popular vote totals?
- Which election had the most lopsided electoral victory? Who won and what percent of the total electoral vote did they receive?
- Make a table that shows each presidential candidate along with a count of the number of elections that they were on the ballot for. Make a bar chart that shows any candidate who appeared more than once.
- For elections with popular vote totals, what percent of the total US population voted? Create a line chart that shows this percent over time.
- Calculate a count of the month born for all US candidates.
- Create a new column in the elections_results table that adds the age of the candidate in the year of the election. (need to add election_date to elections table)

## Notes

Add to future versions:
- us_population_eligible_voter estimate
- election_date to elections table for age calculations