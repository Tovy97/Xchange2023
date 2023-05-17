import os

from envyaml import EnvYAML
from mock import MagicMock


def test_ingest_data() -> None:
    env = EnvYAML('env.yaml')
    os.environ.update(env.export())

    os.environ.update({'ARCHIVE': 'False'})  # for debugging

    cloud_event = MagicMock()

    cloud_event.specversion = "1.0"
    cloud_event.id = "7728828184531124",
    cloud_event.source = "//storage.googleapis.com/projects/_/buckets/xchange-23",
    cloud_event.type = "google.cloud.storage.object.v1.finalized",
    cloud_event.datacontenttype = "application/json",
    cloud_event.subject = "objects/orders_to_ingest-2023_05_17-18_24_32-620642.zip",
    cloud_event.time = "2023-05-17T16:24:41.718922Z",
    cloud_event.bucket = "xchange-23",
    cloud_event.data = {
        "kind": "storage#object",
        "id": "xchange-23/orders_to_ingest-2023_05_17-18_24_32-620642.zip/1684340681709664",
        "selfLink": "https://www.googleapis.com/storage/v1/b/xchange-23/o/orders_to_ingest-2023_05_17-18_24_32-620642.zip",
        "name": "orders_to_ingest-2023_05_17-18_24_32-620642.zip",
        "bucket": "xchange-23",
        "generation": "1684340681709664",
        "metageneration": "1",
        "contentType": "application/octet-stream",
        "timeCreated": "2023-05-17T16:24:41.718Z",
        "updated": "2023-05-17T16:24:41.718Z",
        "storageClass": "STANDARD",
        "timeStorageClassUpdated": "2023-05-17T16:24:41.718Z",
        "size": "650753",
        "md5Hash": "rt6k2L3Lqw+7BUTofvC1uA==",
        "mediaLink": "https://storage.googleapis.com/download/storage/v1/b/xchange-23/o/orders_to_ingest-2023_05_17-18_24_32-620642.zip?generation=1684340681709664&alt=media",
        "crc32c": "0BXt4A==",
        "etag": "COCwjoDi/P4CEAE="
    }

    from main import ingest_data

    ingest_data(cloud_event)


if __name__ == "__main__":
    test_ingest_data()
