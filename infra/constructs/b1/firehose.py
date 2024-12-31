from typing import TypedDict

import aws_cdk as cdk

from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_ssm as ssm
from aws_cdk.aws_kinesisfirehose import CfnDeliveryStream
from constructs import Construct
from typing_extensions import NotRequired
from typing_extensions import Unpack


class Params(TypedDict):
    """Parameters for the B1PubSubFirehose class."""

    bucket: s3.Bucket
    event_bus: events.EventBus
    buffer_interval_in_seconds: NotRequired[int]
    buffer_size_in_m_bs: NotRequired[int]


class B1Firehose(Construct):
    """Ingest events from SNS into S3 with Firehose."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs: Unpack[Params],
    ) -> None:
        super().__init__(scope, id)

        # Read the kwargs
        bucket = kwargs["bucket"]
        event_bus = kwargs["event_bus"]
        buffer_interval_in_seconds = kwargs.get("buffer_interval_in_seconds", 60)
        buffer_size_in_m_bs = kwargs.get("buffer_size_in_m_bs", 64)

        # Create Log Group and Log Stream for kinesis Firehose
        log_group = logs.LogGroup(
            scope=self,
            id="S3DeliveryStreamLogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
        )
        log_stream = logs.LogStream(
            scope=self,
            id="S3DeliveryStreamLogStream",
            log_group=log_group,
        )

        # Create role and policies for Kinesis Firehose
        delivery_role = iam.Role(
            scope=self,
            id="DeliveryRole",
            assumed_by=iam.ServicePrincipal(service="firehose.amazonaws.com"),  # type: ignore
        )

        # Allow Firehose to write to S3
        bucket.grant_write(delivery_role)

        # Allow Firehose to write to CloudWatch Logs
        log_group.grant_write(delivery_role)

        # Create Firehose inline processing configuration
        # Extracts metadata from the event to be used in the prefix
        processing_configuration = CfnDeliveryStream.ProcessingConfigurationProperty(  # noqa: B950
            enabled=True,
            processors=[
                CfnDeliveryStream.ProcessorProperty(
                    type="AppendDelimiterToRecord",
                    parameters=[
                        CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="Delimiter",
                            parameter_value="\\n",
                        ),
                    ],
                ),
                CfnDeliveryStream.ProcessorProperty(
                    type="MetadataExtraction",
                    parameters=[
                        CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="MetadataExtractionQuery",
                            parameter_value='{source:.source, detail_type:."detail-type"}',  # noqa: B950
                        ),
                        CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="JsonParsingEngine",
                            parameter_value="JQ-1.6",
                        ),
                    ],
                ),
            ],
        )

        # Create S3 Destination configuration for Firehose
        s3_destination = CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(  # noqa: B950
            bucket_arn=bucket.bucket_arn,
            role_arn=delivery_role.role_arn,
            buffering_hints=CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=buffer_interval_in_seconds,
                size_in_m_bs=buffer_size_in_m_bs,
            ),
            cloud_watch_logging_options=CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name=log_group.log_group_name,
                log_stream_name=log_stream.log_stream_name,
            ),
            compression_format="GZIP",
            dynamic_partitioning_configuration=CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                enabled=True,
                retry_options=CfnDeliveryStream.RetryOptionsProperty(
                    duration_in_seconds=10
                ),
            ),
            processing_configuration=processing_configuration,
            error_output_prefix="errors/!{firehose:error-output-type}/date=!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/",
            prefix="!{partitionKeyFromQuery:source}/!{partitionKeyFromQuery:detail_type}/date=!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/",
        )

        # Create Firehose Delivery Stream
        delivery_stream = CfnDeliveryStream(
            scope=self,
            id="S3DeliveryStream",
            delivery_stream_type="DirectPut",
            delivery_stream_encryption_configuration_input=CfnDeliveryStream.DeliveryStreamEncryptionConfigurationInputProperty(
                key_type="AWS_OWNED_CMK"
            ),
            extended_s3_destination_configuration=s3_destination,
        )

        # Add dependencies to guarantee that the role, log stream,
        # and group are created before firehose
        delivery_stream.node.add_dependency(delivery_role)
        delivery_stream.node.add_dependency(log_stream)
        delivery_stream.node.add_dependency(log_group)

        # Create event Bridge rule to send all events to Firehose
        events.Rule(
            scope=self,
            id="S3DeliveryStreamRule",
            event_bus=event_bus,
            event_pattern=events.EventPattern(account=[cdk.Aws.ACCOUNT_ID]),
            targets=[
                targets.KinesisFirehoseStream(stream=delivery_stream),
            ],  # type: ignore
        )

        # Export Firehose ARN
        ssm.StringParameter(
            scope=self,
            id="S3DeliveryStreamArn",
            string_value=delivery_stream.attr_arn,
            description="S3 Delivery Stream ARN",
            parameter_name="/pubsub/s3-delivery-stream/arn",
        )
