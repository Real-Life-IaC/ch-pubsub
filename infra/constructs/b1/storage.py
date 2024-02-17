import aws_cdk as cdk

from aws_cdk import aws_s3 as s3
from aws_cdk import aws_ssm as ssm
from constructs import Construct


class B1Storage(Construct):
    """Store events in S3 and notify a topic when object is created"""

    def __init__(
        self,
        scope: Construct,
        id: str,
    ) -> None:
        super().__init__(scope, id)

        access_logs_bucket = s3.Bucket.from_bucket_arn(
            scope=self,
            id="AccessLogsBucket",
            bucket_arn=ssm.StringParameter.value_for_string_parameter(
                scope=self,
                parameter_name="/platform/access-logs/bucket/arn",
            ),
        )

        # Create events Bucket
        self.bucket = s3.Bucket(
            scope=scope,
            id="Bucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="S3Logs/",
            transfer_acceleration=False,
            # Sends events to the default event bus
            event_bridge_enabled=True,
        )

        self.bucket.add_lifecycle_rule(
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                    transition_after=cdk.Duration.days(amount=60),
                )
            ],
        )

        # Add bucket ARN to SSM
        ssm.StringParameter(
            scope=self,
            id="BucketArn",
            string_value=self.bucket.bucket_arn,
            description="Events Bucket ARN",
            parameter_name="/pubsub/bucket/arn",
        )
