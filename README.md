# PDF-Document-Conversion-in-the-cloud
[Assignment 1.pdf](https://github.com/zoharlev1/PDF-Document-Conversion-in-the-cloud/files/9338261/Assignment.1.pdf)


The purpose of the assignment was the get familiar with the Amazon web services and also to learn the Map-Reduce pattern for handling big data. 
Description: The client updates PDF to S3, and makes a connection with sqs (messaing queues) with the job manager. 
The Manager, was responsible for creating splits, and each split was assignted to one worker. 
The worker on the other hand, did the local work, and sent it back to the manager. 
In the end, after all the local jobs were done, the managed reduced it to one PDF and sent back to the client.
