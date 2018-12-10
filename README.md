![Title](images/title_page.png)

## Summary
I created Inside the Vote, a tool that will make Washington State Legislature more transparent and accessible to the everyday citizen. Using machine learning and statistical simulations I predict the probability that current State Senators and Representatives will vote "yea" on upcoming bills, and the probability that the bills will pass. Technologies used include Python, PostgreSQL, Selenium, Beautiful Soup and Amazon Web Services. 

## File Structure Summary
Directory | Description
------------ | -------------
data | example data used in this project
images | EDA and README figures
website | html, css, fonts, js, plugins for website
modeling | final model files
data aquisition | web scraping and API functions
data cleaning | data cleaning pipeline


## Business Question

Let's face it: local government is impossible to follow. Washington State Congress introduced over **1400** bills and passed **685** of them **in just the last year**. It's a full time job to keep up! You have to be updated on the party structure, current senators and representatives, district trends, current and past legislation and industry-specific knowlegde to even begin to make heads and tails of the new proposed bills. On top of that, bills are full of legalese and government jargon, making them inaccessibe to the average citizen. 

This tool will open the door for the everyday citizen to follow and weigh in on what's happening right here in Washington. A quick glance at the break down of sponsors and projected votes will equip Washingtonians to make their voice count.

## Data Collection and Understanding

### Data Collection
![Data Collection](images/data_sources.png)

 - Washington State Legislative Web Services provided the bulk of the data used in this project. Data from this API included:
  - bill features (name, type, dates, urls to text)
  - bill status
  - bill sponsors (primary, secondary)
  - current and past legislators (id, party, name, district)
- Other data was scraped from the following website:
 - http://apps.leg.wa.gov/billsbytopic/default.aspx?year=2019 provided topic labels for each bill
 - The API provided a url for each bill. These were scraped to obtain the bill text.
- A few tables were created manually by me:
 - There were missing legislators from the API. I filled these in manually using this report: http://leg.wa.gov/History/Legislative/Documents/MembersOfLeg2018.pdf
 - I manually grouped the 1700 bill topics in to ~370 major categories
 - I created a table to record which party was in the minority each year

### Data Understanding
- 100,000 bills were used, dating back to 1991
- Voting history existed for roughly 20,000 bills since 1991
- WA State legislation operates in bienniums (two year periods). An example is '1995-96'

## Modeling

### Train-test split:
- Bills with vote dates before 2018 were using for training, bills with vote dates in 2018 were used as the test set.

### Feature Engineering
![Influence Score](images/InfluenceScore.png)
#### Features that improved the model
- is_primary_sponsor
- is_secondary_sponsor
- primary_sponsor_party
- rep_score
- dem_score
- percent_yea
- loyalty_score
- bill_length
- years_until_vote
- num_sections

#### Features that did not improve the model and were discarded
- controversy_score
- Natural Language Processing on bill text
- Bill topic data

### Modeling to Identify Top Power-Middle Influencers:
Baseline Model 

![Influence Score](images/baseline_model.png)

Logistic Regression
Random Forest Classifier
Gradient Boosting Classifier


