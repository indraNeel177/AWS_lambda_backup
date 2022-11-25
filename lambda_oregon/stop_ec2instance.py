import boto3
region = 'us-west-2'
instances = ['i-0033895cb10ced460']
ec2 = boto3.client('ec2', region_name=region)

def lambda_handler(event, context):
    ec2.stop_instances(InstanceIds=instances)
    print('stopped your instances: ' + str(instances))