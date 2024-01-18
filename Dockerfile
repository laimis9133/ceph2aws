FROM python:3.9-alpine

WORKDIR /app

RUN apk --no-cache add \
    groff \
    less \
    && pip install --no-cache-dir boto3 awscli

RUN mkdir /root/.aws
COPY ./credentials /root/.aws/credentials
COPY ./s3_multipart_upload.py /app

ENV ceph_bucket=""
ENV object_path=""
ENV aws_bucket=""
ENV part_size=""
ENV max_parallel_processes=""
ENV tag=""
ENV max_retries=""
ENV chunk_size=""

ENTRYPOINT ["sh", "-c", "python /app/s3_multipart_upload.py ${ceph_bucket} ${object_path} ${aws_bucket} ${part_size} ${max_parallel_processes} ${tag} ${max_retries} ${chunk_size}"]
