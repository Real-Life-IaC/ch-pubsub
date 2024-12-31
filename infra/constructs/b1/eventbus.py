import aws_cdk as cdk

from aws_cdk import aws_events as events
from aws_cdk import aws_eventschemas as schemas
from aws_cdk import aws_ssm as ssm
from constructs import Construct


class B1EventBus(Construct):
    """Event Bus for PubSub"""

    def __init__(
        self,
        scope: Construct,
        id: str,
    ) -> None:
        super().__init__(scope, id)

        # Create Event Bus
        self.event_bus = events.EventBus(
            scope=self,
            id="EventBus",
        )

        # Create an archive for the event bus
        self.event_bus.archive(
            id="Archive",
            description="PubSub Archive",
            event_pattern=events.EventPattern(account=[cdk.Aws.ACCOUNT_ID]),
            retention=cdk.Duration.days(90),
        )

        # Allows EventBridge to automatically discover schemas
        schemas.CfnDiscoverer(
            scope=self,
            id="EventSchemaDiscoverer",
            source_arn=self.event_bus.event_bus_arn,
            description="PubSub Event Schema Discoverer",
        )

        # Export to SSM
        ssm.StringParameter(
            scope=self,
            id="EventBusArn",
            parameter_name="/pubsub/event-bus/arn",
            string_value=self.event_bus.event_bus_arn,
        )

        ssm.StringParameter(
            scope=self,
            id="EventBusName",
            parameter_name="/pubsub/event-bus/name",
            string_value=self.event_bus.event_bus_name,
        )
