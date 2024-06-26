## Simple script to upload files with mp from Ceph S3 to AWS S3 via boto3
## It expects the following file/object naming pattern: ceph_bukcet/object_path/object_name_with_date_YYYY_MM_DD

import boto3
import sys
import logging
import hashlib
import re
from datetime import datetime
from multiprocessing import Process, Manager, Semaphore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

if __name__ == "__main__":
    if len(sys.argv) != 7:
        logger.error("Usage: python3 this-script.py ceph_bucket object_path aws_bucket part_size(MB) max_parallel_processes tag")
        sys.exit(1)

    ceph_bucket = sys.argv[1]
    object_path = sys.argv[2]
    aws_bucket = sys.argv[3]
    part_size = int(sys.argv[4]) * 1024 * 1024
    max_parallel_processes = int(sys.argv[5])
    tag = sys.argv[6]

# Tweaking params
max_retries = 5 # Retries for network errors
chunk_size = 100 #AKA maximum number of completed processes before releasing resources (keep less than OS open file limit - required for massive (TB) files)
ceph_s3_client = boto3.Session(profile_name='ceph').client('s3') # Requires aws-cli with Ceph bucket keys in .aws/credentials
aws_s3_client = boto3.Session(profile_name='default').client('s3') # Requires aws-cli with AWS bucket keys in .aws/credentials


# Functions
# Function to get latest object_key from path (according to date in object name, not creation time)
def find_latest_file(bucket, directory, file_pattern):
    # List all objects in the bucket with the specified prefix
    response = ceph_s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{directory}/"
    )

    # Extract files matching the pattern and containing a date in the format YYYY-MM-DD
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].startswith(f"{directory}/{file_pattern}") and re.search(r'(\d{4}-\d{2}-\d{2})', obj['Key'])]
    if not files:
        return None

    # Extract dates from filenames and find the latest file
    latest_file = max(files, key=lambda x: re.search(r'\d{4}-\d{2}-\d{2}', x).group())
    return latest_file

# Function to get and upload object split into parts - read and write logic is here
def process_part_number(part_number, part_size, ceph_bucket, object_key, upload_id, manager_dict, semaphore):
    start_byte = (part_number - 1) * part_size
    end_byte = min(part_number * part_size - 1, total_size - 1)
    # Read part from Ceph S3 with retries in case of failure
    logger.info("Reading part %d of object. Range: %d-%d", part_number, start_byte, end_byte)
    for retry in range(max_retries):
        try:
            response_ceph = ceph_s3_client.get_object(
                Bucket=ceph_bucket,
                Key=object_key,
                Range=f'bytes={start_byte}-{end_byte}'
            )
            part_data = response_ceph['Body'].read()
            content_hash = hashlib.md5(part_data).hexdigest()
            logger.info(f"Object part {part_number} succesfully retrieved after {retry} retries. Hash: {content_hash}")
            break
        except Exception as e:
            logger.warning(f"Error getting part {part_number}. Retrying... ({retry + 1}/{max_retries})")
            if retry == max_retries -1:
                logger.error(f"Maximum number of retries reached. Unable to retrieve the object part {part_number}. Exiting: {e}")
                sys.exit()
    # Upload part to AWS S3 with retries in case of failure
    for retry in range(max_retries):
        try:
            response_aws = aws_s3_client.upload_part(
                Bucket=aws_bucket,
                Key=object_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=part_data
            )
            etag = response_aws['ETag'] # Check if Hash and ETag match between Ceph and AWS after upload
            if etag.strip('"') != content_hash:
                logger.error(f"Hashes (md5) are different for {part_number} after upload. Hash value (in Ceph): {content_hash}, ETag value (in AWS): {etag}.")
                sys.exit(1)
            # Add ETag to dictionary. Later used for multipart upload closure
            etag_dict = {'PartNumber': part_number, 'ETag': etag}
            manager_dict[part_number] = etag_dict
            logger.info(f"Done uploading for part {part_number}. ETag: {etag_dict}")
            break
        except Exception as e:
            logger.warning(f"Error uploading part {part_number}. Retrying... ({retry + 1}/{max_retries})")
            if retry == max_retries - 1:
                logger.error(f"Max retries reached. Upload failed for part {part_number}. Error: {e}")
                sys.exit()

    semaphore.release() # Parallelism for faster upload

# Multiprocessing - launch parallel processes, control max with semaphore
def create_and_run_processes(start_part_number, end_part_number):
    processes = []
    semaphore = Semaphore(max_parallel_processes)
    for part_number in range(start_part_number, end_part_number):
        semaphore.acquire()
        process = Process(target=process_part_number, args=(part_number, part_size, ceph_bucket, object_key, upload_id, etag_manager_dict, semaphore))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()
        process.close()
    logger.info(f"Releasing resources after part number {end_part_number}.")


# Final adjustment for various backup name patterns
# This is just re-use same code (container) for multiple services
# Just set file_pattern = 'object_name' if not required
if object_path == 'foo':
    file_pattern = 'bar'
elif object_path == 'foo2':
    file_pattern = 'bar2'
else:
    file_pattern = 'backup' # Some default backup naming pattern




# Ok. Actual execution starts here
# Get exact object name to upload
object_key = find_latest_file(ceph_bucket, object_path, file_pattern)
logger.info(f"Found latest object to upload: {object_key}")

# Get source object total size
metadata_response = ceph_s3_client.head_object(Bucket=ceph_bucket, Key=object_key)
total_size = metadata_response['ContentLength']
logger.info(f"Found object metadata. Object size: {total_size}")

# Initiate the multipart upload on AWS S3
logger.info(f"Starting multipart upload for {object_key} from Ceph {ceph_bucket} bucket to AWS {aws_bucket} bucket.")
logger.info(f"Will use configured {sys.argv[4]}MB ({part_size}B) as single part size with {max_parallel_processes} parallel threads.")
response = aws_s3_client.create_multipart_upload(
    Bucket=aws_bucket,
    Key=object_key
)
upload_id = response['UploadId']
logger.info(f"Created multipart request with Upload ID: {upload_id}")

# Set an empty ETag list and set Manager to gather lists from processes. This is required for AWS multipart uploads.
etag_list = []
manager = Manager()
etag_manager_dict = manager.dict()

# Split total object parts into chunks and then upload it
total_chunks = (total_size // part_size) // chunk_size
for chunk in range(total_chunks):
    start_part_number = chunk * chunk_size + 1
    end_part_number = (chunk + 1) * chunk_size + 1
    create_and_run_processes(start_part_number, end_part_number)

# Handle (upload) remaining object bites that don't fit nicely into a full chunk 
remaining_start_part_number = total_chunks * chunk_size + 1
remaining_end_part_number = int(total_size / part_size) + 2
create_and_run_processes(remaining_start_part_number, remaining_end_part_number)

# Sort ETags - required for AWS to complete multipart upload
etag_manager_dict_sorted = dict(sorted(etag_manager_dict.items(), key=lambda item: item[1]['PartNumber']))

# Complete the multipart upload
aws_s3_client.complete_multipart_upload(
    Bucket=aws_bucket,
    Key=object_key,
    UploadId=upload_id,
    MultipartUpload={'Parts': [{'PartNumber': key, 'ETag': value['ETag']} for key, value in etag_manager_dict_sorted.items()]}
)

# Add tag to the uploaded object - for any AWS S3 lifecycle rules based on tags
aws_s3_client.put_object_tagging(
    Bucket=aws_bucket,
    Key=object_key,
    Tagging={
        'TagSet': [
            {
                'Key': 'GLACIER_AFTER',
                'Value': tag
            },
        ]
    }
)

logger.info(f"Multipart upload to AWS {aws_bucket} bucket completed for: {object_key}")
