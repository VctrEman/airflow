#import configparser
import json
import boto3
import pandas as pd

#Get and Put methods for S3 data
def get_from_s3(origin_bucket_name, Key, access_key, secret_key, aws_endpoint = None):
    s3 = boto3.client('s3',
                        aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key)
    obj = s3.get_object(Bucket= origin_bucket_name, Key = Key)
    print("Extracted from","s3a://{}/{}".format(origin_bucket_name, Key) ) 
    return obj

def dump_to_s3( Body, destination_bucket_name, Key, access_key, secret_key, aws_endpoint = None):
    s3 = boto3.client('s3',
                        aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key)
    s3.put_object(Body = Body, Bucket = destination_bucket_name, Key= Key)
    print("Put into","s3a://{}/{}".format(origin_bucket_name, Key) )

# load the aws_boto_credentials values
with open("/opt/airflow/data/config.json", "r") as read_file:
    cfg = json.load(read_file)

#setting configurations
s3_config = cfg['aws_boto_credentials']
access_key = s3_config['access_key']
secret_key = s3_config['secret_key']
origin_bucket_name = s3_config['landing_layer_bucket']
destination_bucket_name = s3_config['raw_layer_bucket']
                            
s3 = boto3.client('s3',
                    aws_access_key_id = access_key,
                    aws_secret_access_key = secret_key)

origin_s3_file = "order_extract.csv"

obj = get_from_s3( access_key = access_key,
                    secret_key = secret_key,
                    origin_bucket_name = origin_bucket_name,
                    Key= origin_s3_file)

df = pd.read_csv(obj['Body'], sep = '|')
print(df.info())
to_csv = df.to_csv(index=False, header=False)

dump_to_s3( destination_bucket_name = destination_bucket_name, 
            Body = to_csv, 
            Key = "raw_"+origin_s3_file,
            access_key=access_key, 
            secret_key=secret_key)

print('Finished!')
