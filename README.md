# bigquery_google_ads_queries
This is a public repo of my favorite BigQuery queries for the native backfill with Google Ads


BigQuery TypeScript Query Helper

This project is designed to assist users in writing and executing SQL queries for Google BigQuery using TypeScript to help ChatGPT tweak your queries. You can easily create and modify queries for your Google Ads data, with a focus on customization and flexibility.

Getting Started

Prerequisites

	1.	Set up Google BigQuery Data Transfer Service
Before running any queries, you’ll need to set up the transfer of data from Google Ads to BigQuery. Follow the guide provided by Google to configure the transfer:
Set up Google Ads Data Transfer

	2.	Google Cloud Project
Ensure you have a Google Cloud project with BigQuery enabled. You’ll need the project ID and dataset ID where the Google Ads data is stored.


Example Query

Below is an example query written in TypeScript that retrieves Google Ads performance data. You can customize it by replacing the variables to suit your needs.

export const queryAccount = function (projectId: string, accountIdNumber: number, daysStart: string, daysEnd: string) {
    return `SELECT
                        round(SUM(c.Conversions), 2) AS Conversions,
                        round((SUM(c.Cost) / 1000000), 2) AS Cost,
                        (CASE WHEN sum(c.Conversions) > 1 THEN round(((SUM(c.Cost)) / SUM(c.Conversions)),2) ELSE 0 END)AS CPA
                    FROM
                        \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` c
                    WHERE c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                    ORDER BY
                        Conversions DESC`;
};


You can use the following prompt to ask ChatGPT (or another AI-based code assistant) to help replace the variables:

Here is a TypeScript query for BigQuery:

export const queryAccount = function (projectId: string, accountIdNumber: number, daysStart: string, daysEnd: string) {
    return `SELECT
                        round(SUM(c.Conversions), 2) AS Conversions,
                        round((SUM(c.Cost) / 1000000), 2) AS Cost,
                        (CASE WHEN sum(c.Conversions) > 1 THEN round(((SUM(c.Cost)) / SUM(c.Conversions)),2) ELSE 0 END)AS CPA
                    FROM
                        \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` c
                    WHERE c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                    ORDER BY
                        Conversions DESC`;
};

Please replace the variables as follows:
- projectId: Your Google Cloud project ID
- accountIdNumber: Your Google Ads account ID
- daysStart: Start date for the data range (format: YYYY-MM-DD)
- daysEnd: End date for the data range (format: YYYY-MM-DD)

Once the variables are replaced, return the SQL query that can be executed in BigQuery.


Running the Query in BigQuery

After customizing the query with your own variables, simply take the resulting SQL and paste it into the BigQuery interface to run it:

	1.	Go to Google BigQuery
	2.	Paste your query into the Query Editor.
	3.	Click Run.

Contributing

Feel free to contribute by creating pull requests, opening issues, or suggesting features.

This version includes everything in a single README file, ready to add to your GitHub project.


Visualizing Data in Looker Studio

Once your query is executed and you have the desired data in BigQuery, you can visualize it in Looker Studio (formerly Google Data Studio). Follow the steps below to create a report and visualize the query results:

Step 1: Connect Looker Studio to BigQuery

	1.	Go to Looker Studio.
	2.	Click on Create → Data Source.
	3.	Select BigQuery from the list of available connectors.
	4.	Choose Custom Query instead of selecting an existing table.
	5.	Paste your SQL query here.
	Note: Replace your daysStart and daysEnd variables in the query with the following dynamic date range parameters to allow Looker Studio to filter data based on the report’s date range:

For example, your query should look like this:

SELECT
     round(SUM(c.Conversions), 2) AS Conversions,
     round((SUM(c.Cost) / 1000000), 2) AS Cost,
     (CASE WHEN sum(c.Conversions) > 1 THEN round(((SUM(c.Cost)) / SUM(c.Conversions)), 2) ELSE 0 END) AS CPA,
     FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y-%m-%d', @DS_START_DATE)) AS Start_Date,
     FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y-%m-%d', @DS_END_DATE)) AS End_Date
 FROM
     `${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}` c
 WHERE c._PARTITIONDATE BETWEEN PARSE_DATE('%Y-%m-%d', @DS_START_DATE) AND PARSE_DATE('%Y-%m-%d', @DS_END_DATE)
 ORDER BY Conversions DESC;


