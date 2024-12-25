import json
import os

from typing import cast

import boto3

from mimesis import Generic
from mimesis.locales import Locale
from mypy_boto3_events import EventBridgeClient


# Initialize EventBridge client
client = cast(EventBridgeClient, boto3.client("events"))

# Use mimesis to generate random data
generic = Generic(locale=Locale.EN)

for _ in range(2000):
    # Create a random json object
    data = {
        "email": generic.person.email(),
        "name": generic.person.full_name(),
        "created_at": generic.datetime.datetime(),
    }

    # Put the event on the Event Bus
    client.put_events(
        Entries=[
            {
                "Source": "localTest",
                # Add the name of your event bus
                "EventBusName": os.getenv("EVENT_BUS_NAME", ""),
                "DetailType": "eventTested",
                "Detail": json.dumps(data, default=str),
            }
        ]
    )

    if _ % 100 == 0:
        print(f"Sent {_} events")  # noqa: T201
