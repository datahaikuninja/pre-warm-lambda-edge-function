import argparse
import boto3

parser = argparse.ArgumentParser()
parser.add_argument('--ecs_cluster', type=str, required=True,
                    help='ECS cluster name')
parser.add_argument('--ecs_service', type=str, required=True,
                    help='ECS service name')
parser.add_argument('--profile', type=str, required=True,
                    help='AWS profile name')
args = parser.parse_args()

session = boto3.Session(profile_name=args.profile, region_name='ap-northeast-1')
ecs = session.client('ecs')


def stop_prewarm(cluster_name, service_name):
    ecs.update_service(
            cluster=cluster_name,
            service=service_name,
            desiredCount=0
    )


def main():
    stop_prewarm(args.ecs_cluster, args.ecs_service)


if __name__ == '__main__':
    main()
