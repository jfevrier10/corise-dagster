from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock],
    description="List of Stocks")},
)
def get_s3_data(context):
    output = list()
    context.log.info(f's3:{context.resources.s3.bucket}')
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
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
