from constructs import Construct
from infra.constructs.b1.firehose import B1PubSubFirehose
from infra.constructs.b1.storage import B1PubSubStorage


class B2PubSub(Construct):
    """Store events from topics in S3"""

    def __init__(
        self,
        scope: Construct,
        id: str,
    ) -> None:
        super().__init__(scope, id)

        storage = B1PubSubStorage(scope=self, id="Storage")

        B1PubSubFirehose(scope=self, id="Firehose", bucket=storage.bucket)
