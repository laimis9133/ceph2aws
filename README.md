# ceph2aws
Upload objects from Ceph S3 buckets to AWS S3 buckets using customizable multipart upload via Python boto3.
Useful for backup or general data movement purposes. I havent seen one clean solution so here you go, maybe it will help.

You can setup scheduled uploads using Kubernetes cronjobs and customize details like part sizes, number of parallel processes, etc.


## How to
For single upload use the python script with the following variables:  
`python3 s3_multipart_upload.py ceph_bucket object_path aws_bucket part_size(MB) max_parallel_processes tag`  
Tweak object name patterns within the .py script to suit your case (from line 49).  
Script expects objects to contain YYYY-MM-DD dateformat within their name and searches for a latest file/object based on that.  

For Kubernetes cronjob deployment add your Ceph and AWS keys to the credentials file and build a image from Dockerfile.  
Push the image to your container registry (like GitLab).  
Create a secret in Kubernetes cluster for container registry authentication (access token base64 encoded).  
Reference the secret and the registry image within the deployment .yaml file as well as the cron schedule.  

For a more GitOps approach keep everything within same repository (as now) and let the gitlab CI do the deployment when a push to master happens. Useful if you want to change a schedule or modify details.

### For the Future
Add part size constrains to limit parts to max 10000 (AWS limitation)  
Add some way to split file if its larger than 5TB (AWS limitation)
