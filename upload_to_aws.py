# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 17:30:59 2020

@author: EILAP6639
"""
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import db_config
import psycopg2
from datetime import datetime
import logging
def upload_to_aws(local_file, bucket_name, object_name, req_input):

    s3 = boto3.client('s3', aws_access_key_id=db_config.ACCESS_KEY,
                      aws_secret_access_key=db_config.SECRET_KEY)

    try:
        s3.upload_file(local_file, db_config.bucket_name, object_name)
        print("Upload Successful")
        # return True
    except FileNotFoundError:
        print("The file was not found")
        # return False
    except NoCredentialsError:
        print("Credentials not available")
        # return False
    except Exception as e:
        print(e)


    # def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """
    requestid = req_input["requestid"]
    modified_time=datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
    
    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': db_config.bucket_name,
                                                            'Key': object_name})
        conn =psycopg2.connect(dbname=db_config.redshift_dbname, host=db_config.redshift_host, port=db_config.redshift_port, user=db_config.redshift_user, password=db_config.redshift_password)
        conn.autocommit = True
        cur = conn.cursor();
        
        updateQuery = """update table set col4='{0}',col2='completed', col5='{1}' where col1='{2}' and col2='pending';""".format(response, modified_time, requestid)
        cur.execute(updateQuery)
        cur.close() 
        conn.close()
        
    except ClientError as e:
        logging.error(e)
        print(e)