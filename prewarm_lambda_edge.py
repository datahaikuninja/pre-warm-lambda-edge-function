from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from logging import getLogger, StreamHandler, INFO
import argparse
import time
import threading
import sys
import textwrap
import boto3

logger = getLogger(__name__)
logger.setLevel(INFO)
ch = StreamHandler()
ch.setLevel(INFO)
logger.addHandler(ch)

parser = argparse.ArgumentParser()
parser.add_argument('--ecs_cluster', type=str, required=True,
                    help='ECS cluster name')
parser.add_argument('--ecs_service', type=str, required=True,
                    help='ECS service name')
parser.add_argument('--target_number', type=int, required=True,
                    help='Target number of concurrent executions')
parser.add_argument('--function_name', type=str, required=True,
                    help='Lambda@Edge function name to get concurrent executions metrics.\
                          Function name must be in the format of "us-east-1:name"')
parser.add_argument('--profile', type=str, required=True,
                    help='AWS profile name')
args = parser.parse_args()

session = boto3.Session(profile_name=args.profile, region_name='ap-northeast-1')
cloudwatch = session.resource('cloudwatch')
metric = cloudwatch.Metric('AWS/Lambda', 'ConcurrentExecutions')
ecs = session.client('ecs')
waiter = ecs.get_waiter('services_stable')


def waiter_caller(ecs_cluster_name: str, ecs_service_name: str):
    try:
        waiter.wait(
                cluster=ecs_cluster_name,
                services=[ecs_service_name],
                WaiterConfig={
                    'Delay': 10,
                    'MaxAttempts': 30
                }
        )
    except Exception:
        logger.exception('Deploy failed')
        logger.info('Please check ECS service status, and try again.')
        sys.exit(1)


def get_current_desired_count(ecs_cluster_name: str, ecs_service_name: str) -> int:
    response = ecs.describe_services(
            cluster=ecs_cluster_name,
            services=[ecs_service_name]
    )
    return response['services'][0]['desiredCount']


def update_ecs_service(ecs_cluster_name: str, ecs_service_name: str):
    current_desired_count = get_current_desired_count(ecs_cluster_name, ecs_service_name)
    ecs.update_service(
            cluster=ecs_cluster_name,
            service=ecs_service_name,
            desiredCount=current_desired_count + 1,
    )
    t = threading.Thread(target=waiter_caller, args=(ecs_cluster_name, ecs_service_name))
    t.start()
    while t.is_alive():
        logger.info('Waiting for ECS service to be stable...')
        time.sleep(10)


def print_statistics(current_value: float, target_value: float):
    logger.info(textwrap.dedent(f'''
        ====== ConcurrentExecutions Statistics ======
        Current value: {current_value}
        Target value: {target_value}
        =============================================''').strip())


def main():
    while True:
        metrics_data = metric.get_statistics(
                Dimensions=[{'Name': 'FunctionName', 'Value': args.function_name}],
                StartTime=datetime.now(ZoneInfo('Asia/Tokyo')) - timedelta(seconds=300),
                EndTime=datetime.now(ZoneInfo('Asia/Tokyo')),
                Period=60,
                Statistics=['Maximum']
        )
        try:
            # ５分間に取得したいくつかの最大値のデータポイントの平均を取得してもいいかもしれない
            # concurrent_executionsのグラフはリニアに増加せず、急に減少・増加することがある（グラフがギザギザになる）
            latest_concurrent_executions = sorted(
                    metrics_data['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[0]['Maximum']
        except IndexError:
            logger.exception('No datapoint found. Please check if the function is invoked.')  # datapointが存在しない場合はエラー
            sys.exit(1)

        if latest_concurrent_executions < float(args.target_number):
            print_statistics(latest_concurrent_executions, args.target_number)
            logger.info('Continue pre-warming. Update ECS service to add 1 task.')
            update_ecs_service(args.ecs_cluster, args.ecs_service)
            logger.info('ECS service is stable. Retrive concurrent executions after 60 seconds.')
            time.sleep(60)
        else:
            print_statistics(latest_concurrent_executions, args.target_number)
            logger.info('Pre-warming completed.')
            break


if __name__ == '__main__':
    main()
