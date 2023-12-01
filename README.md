Project Overview
This project is focused on transforming txt files into structured csv files, followed by data loading into database tables. The data is then aggregated hourly and daily, utilizing specific KPIs. This repository contains the Backend (BE) components necessary for data processing and database interaction.

System Requirements
Vertica Database
DbVisualizer (or equivalent database management tool)
Any backend runtime environment compatible with the project's programming language
Setup Instructions
Folder Creation:

Create a folder named "Watcher" at C:\Users\User\Desktop\Baby Ni\BackEnd\Watcher.
Database Connection:

Use the following connection string for Vertica Database:
Driver={Vertica};Server=10.10.4.231;Database=test;User=bootcamp7;Password=bootcamp72023;

Database Table Creation:

In DbVisualizer, create the following tables:
TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER
TRANS_MW_ERC_PM_WAN_RFINPUTPOWER
TRANS_MW_AGG_SLOT_HOURLY
TRANS_MW_AGG_SLOT_DAILY
SQL scripts for each table are provided in the sql_scripts folder.
Running the Backend Application:

Run the BE application.
Drop the txt files into the "Watcher" folder.
