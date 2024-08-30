import json
import os

import grpc
import yandexcloud
from tusclient import client
from yandex.cloud.video.v1.channel_pb2 import Channel
from yandex.cloud.video.v1.channel_service_pb2 import CreateChannelMetadata
from yandex.cloud.video.v1.channel_service_pb2 import CreateChannelRequest
from yandex.cloud.video.v1.channel_service_pb2 import ListChannelsRequest
from yandex.cloud.video.v1.channel_service_pb2_grpc import ChannelServiceStub
from yandex.cloud.video.v1.video_pb2 import Video
from yandex.cloud.video.v1.video_service_pb2 import CreateVideoMetadata
from yandex.cloud.video.v1.video_service_pb2 import CreateVideoRequest
from yandex.cloud.video.v1.video_service_pb2 import VideoTUSDParams
from yandex.cloud.video.v1.video_service_pb2_grpc import VideoServiceStub
from yandexcloud.auth import get_auth_token

ORG_ID = os.getenv("ORG_ID")

# We have to monkey-patch the SDK to add the video module to the list of supported modules
yandexcloud._sdk._supported_modules.append(("yandex.cloud.video", "video"))


def main():
    interceptor = yandexcloud.RetryInterceptor(max_retry_count=5, retriable_codes=[grpc.StatusCode.UNAVAILABLE])

    with open("sa.json", "r") as f:
        service_account_key = json.loads(f.read())

    sdk = yandexcloud.SDK(
        interceptor=interceptor,
        service_account_key=service_account_key,
    )
    channel_client = sdk.client(ChannelServiceStub)
    video_client = sdk.client(VideoServiceStub)

    # Find or create a channel
    channels = channel_client.List(ListChannelsRequest(
        organization_id=ORG_ID,
    )).channels

    channels = channels or []

    channel = None
    for c in channels:
        if c.title == "demo-channel":
            channel = c
            break

    if not channel:
        op = channel_client.Create(CreateChannelRequest(
            organization_id=ORG_ID,
            title="demo-channel",
        ))

        operation_result = sdk.wait_operation_and_get_result(
            op,
            response_type=Channel,
            meta_type=CreateChannelMetadata,
        )

        channel = operation_result.response

    file_stats = os.stat("cat.mp4")

    # Use the client to make requests
    op = video_client.Create(CreateVideoRequest(
        channel_id=channel.id,
        title="demo-video",
        description="This is a demo video",
        tusd=VideoTUSDParams(
            file_size=file_stats.st_size,
            file_name="cat.mp4",
        ),
        public_access={},  # Video has public access
    ))

    operation_result = sdk.wait_operation_and_get_result(
        op,
        response_type=Video,
        meta_type=CreateVideoMetadata
    )

    video = operation_result.response

    print(f"Created video: {video}")

    token = get_auth_token(
        service_account_key=service_account_key,
    )

    my_client = client.TusClient(video.tusd.url,
                                 headers={'Authorization': f"Bearer {token}"})

    # Here we MUST use the URL from the video object. If you skip this parameter, the client will try to create a new
    # upload and will fail with a 405 error, because the POST method is not allowed on the video URL.
    uploader = my_client.uploader('cat.mp4', url=video.tusd.url)
    uploader.upload()

    print("Upload complete")


if __name__ == "__main__":
    main()
