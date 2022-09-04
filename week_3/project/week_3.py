from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock],
    description="List of Stocks")},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    output = list()
    key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(key)
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregation with the greatest value",
)
def process_data(stocks):
    output = max(stocks, key= lambda value: value.high)
    return Aggregation(date=output.date,high=output.high)


@op(
    description="Upload aggregations to redis",
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(aggregation.date, str(aggregation.high))


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key: str):
    return {
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    } 

local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")  # Add your schedule

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")  # Add your schedule


@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(bucket='dagster', prefix='prefix', endpoint_url='http://host.docker.internal:4566')

    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return

    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={
                "resources": {
                    "s3": {
                        "config": {
                            "bucket": "dagster",
                            "access_key": "test",
                            "secret_key": "test",
                            "endpoint_url": "http://localstack:4566",
                        }
                    },
                    "redis": {
                        "config": {
                            "host": "redis",
                            "port": 6379,
                        }
                    },
                },
                "ops":{"get_s3_data": {"config": {"s3_key": new_file}}},
            }
        )