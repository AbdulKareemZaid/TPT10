**Twitter Stream Processing Pipeline
Project Overview
**
This project implements a complete data pipeline that handles streaming tweets, processes the data, and stores the processed results in a MongoDB database for efficient querying and visualization.


**Pipeline Components**

    Stream Ingestion:

        Utilizes the Twitter API or a simulated tweet generator to receive a continuous stream of tweets.

        Maintains the incoming tweet stream in an Apache Kafka topic for intermediate storage and scalability.

    Processing Components:

        Transforms and processes tweets to make them searchable over text, time, and space.

        Extracts hashtags and stores them in a nested array within the tweet.

        Uses a sentiment analyzer to extract the sentiment of each tweet based on its text.

    Storage:

        Stores the processed tweets and their metadata in a MongoDB database.

        Designs an appropriate schema in MongoDB to support efficient querying for tweet annotations and visualization.


**Installation and Setup**
Prerequisites

    Java 11 or later

    Scala 2.12.18 or later

    SBT (Scala Build Tool)

    Apache Kafka

    MongoDB

  
Pipeline Architecture

    Stream Ingestion:

        Receives tweets from the Twitter API or simulated tweet generator.

        Stores the tweet stream in an Apache Kafka topic.

    Processing Components:

        Processes tweets to extract fields, hashtags, and sentiment.

        Makes tweets searchable over text, time, and space.

    Storage:

        Stores processed tweets in MongoDB with an efficient schema for querying.

        Allows for visualization and analysis of tweet data.


![Untitled](https://github.com/user-attachments/assets/03d2ec33-6b37-4e3c-9ff5-e1a6beabad1b)

