# Self Service - Data Security Explained

## What is Data Security in Self Service

Data security for self service Business intelligence is an area which has lots of attention these days. There is primarly 2 parts to it one is writing to self service storage and other is reading from it. Writes are usually controlled but reads are the most challenging. This is a topic i hear and talk about it every day.

For Read there are considerstation like

- Number of users accessing the system
- Performance of the system
- How secured the data is?
- Who can access what data.

let's first start with what is self service. Over the years of using data and building reports to run business using data to take decision, it has been done with various technologies. But the challenge has been, data is duplicated in some fashion and stored in systems specific to certain use cases to take decision. This causes lots of delay and time consuming development to get the data so that we can take data focussed decision making.

Another challenge is data stored for accessing when the volume increases it becomes hard to provide security as each row can have multiple security rules or policies. For example when there in few rows it is easy to provide security with performance, now if the same data sets has few billions rows then performance become challenging.

## why Databases other than files?

This is a topic which is kinda important. Files accessing with few rows are easy and nothing is challenging but when the file sizes grows in gigabytes or terabytes and more it becomes hard and time consuming to access. To provide a solution data bases are their technologies are alternate. Not only that conncurrency and multiple user access are well suited in database. Also data bases and related technologies provide storage not for just one application but may be one or more, sharing model.

## Self service Security

Self service avoid that, by placing the data in one place not duplicated but ability to provide data access to build their own reports. Now this also has challenges on how to best store the data in a way that all business users can access it. Also provides another challenge in terms of security since all data is available how do we control security. Data governance is also an important part. Given the data model is common to all user, and having business data which might have sensitive and non sensitive data, which means we need to put a lot of focus in security, encryption, masking, data access policy.

Here are some of the topics for data security

- Data access security for read
    - Row level Security
    - Column level security
- data write security for ETL,ELT jobs. Write access for manual oprations or other applications
- Classifying sensitive data and have a inventory
- Provide masking for data that doesn't have to be displayed like phone number xxx-xxx-xxxx
- Encrypting columns that are considered sensitive, espcially HR related like SSN, Income
- Administration access, where sensitive data should be excluded.
- Ability to audit who access what data
- Ability to see lineage of specific data and which users all accessed

From the above the main is going to be Data access for write and read. Write is mostly controlled by applications using service account which might have elevated access since it has to process all records in a data set. Writes are also controlled by system and password's or secrets can be rotated or kept at secured location.

Read access is where the most challenge is. When we have single source of truth, where all the business data resides. Not all users in a company or enterprise might have access to all the data. So here is where row level and column level security is very important. Now there are options other than Row level and column level but those duplicate data using views, or tables and other means.

To avoid data duplication and have single place to govern the security systems with Row level and column level security are much needed. By organizing the security needs with row level and column level we can provide which users and what access they can view or see. This allow business users to do their own reporting but at the same time data is secured. Not only that when a user is compromised it would be easy to find and change the password or username for that user to make him secured. Not only that we also need to collect audit logs to know how and when data was accessed by who. Will also help remedy comproside users and their accounts faster and timely fashion. When other than user account is used then, it becomes challenging and risk is high.

For Example if access is provided by some other means like service account, system account, or any other means and there are about 200 users access the data and if compromised, all the 200 users are affected and becomes a nuance. Now we have to change all the 200 users compromised account and we also dont how the system was comprised (which user in those 200 was really comprised).

Encrypting columns for sensitive data is also another huge security item. This protects from unncessary or mistaken access provided and also if compromised as well.

This article is intended to information sharing and guidance only.