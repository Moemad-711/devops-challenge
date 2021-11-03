# devops-challenge
This repo provides a pipeline deployable on AWS that automates the Sentiment analysis and database inserting of movies reviews uploaded and stored on an S3 bucket from a client application. The pipeline make use of DynamoDB, S3 and Lambda Function services from AWS. The Sentiment analysis is done in Batch Mode and by using Hugging Face package. The pipeline consists of of the following Lambda Function:
- review-aggregator
- detect-review-lang
- translate-review-text
- analyze-review-text

## Review 
 
