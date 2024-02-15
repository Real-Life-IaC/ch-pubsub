import aws_cdk as cdk

from constructs import Construct
from infra.constructs.b2.pubsub import B2PubSub


class PubSubStack(cdk.Stack):
    """Create the PubSub resources"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        B2PubSub(scope=self, id="PubSub")

        # Add tags to everything in this stack
        cdk.Tags.of(self).add(key="owner", value="Data")
        cdk.Tags.of(self).add(key="repo", value="ch-pubsub")
        cdk.Tags.of(self).add(key="stack", value=self.stack_name)
