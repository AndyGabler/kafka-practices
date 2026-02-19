# Liquibase App

Liquibase application that is used to generate the PostgresSQL database hosted and managed by AWS Aurora.

# AWS Info

Database URL: `swimsetsitedatabasecluster-instance-1.cwz82qyyutw6.us-east-1.rds.amazonaws.com`
Database Name: SwimSetSiteDatabase

## Local Setup

Locally, you will need to add a gradle.properties file.

This file is ignored in Git. This is used to configure the connection details to your local PostgresSQL instance. The values in this follow should be as follows:
```
DATABASE_HOST= # Database host name and port
DATABASE_NAME= # name of the database
DATABASE_USERNAME= # username to sign into database
DATABASE_PASSWORD= # password to sign into database
```