# ceph2aws
Upload objects from Ceph S3 buckets to AWS S3 buckets using customizable multipart upload via Python boto3.
Useful for backup or general movement purposes. I havent seen one clean solution so here you go.

You can setup scheduled uploads using Kubernetes cronjobs and customize details like part sizes, number of parallel processes, etc.

Tweak object name patterns within the multipart upload python script. The script expects objects to contain YYYY-MM-DD dateformat within their name!
