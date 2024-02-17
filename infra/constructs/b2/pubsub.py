from constructs import Construct
from infra.constructs.b1.eventbus import B1EventBus
from infra.constructs.b1.firehose import B1Firehose
from infra.constructs.b1.storage import B1Storage


class B2PubSub(Construct):
    """Store events from topics in S3"""

    def __init__(
        self,
        scope: Construct,
        id: str,
    ) -> None:
        super().__init__(scope, id)

        event_bus = B1EventBus(scope=self, id="EventBus")
        storage = B1Storage(scope=self, id="Storage")

        B1Firehose(
            scope=self,
            id="Firehose",
            bucket=storage.bucket,
            event_bus=event_bus.event_bus,
        )
