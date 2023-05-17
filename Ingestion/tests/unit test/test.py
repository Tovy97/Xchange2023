import os

from envyaml import EnvYAML
from mock import MagicMock


def test_ingest_data() -> None:
    env = EnvYAML('env.yaml')
    os.environ.update(env.export())

    cloud_event = MagicMock()
    cloud_event.specversion = "1.0"
    cloud_event.id = "7718729690826582",
    cloud_event.source = "//storage.googleapis.com/projects/_/buckets/xchange-23",
    cloud_event.type = "google.cloud.storage.object.v1.finalized",
    cloud_event.datacontenttype = "application/json",
    cloud_event.subject = "objects/orders_to_ingest-2023_05_16-19_09_21-282726.zip",
    cloud_event.time = "2023-05-16T17:09:35.315797Z",
    cloud_event.bucket = "xchange-23",
    cloud_event.data = {
        "kind": "storage#object",
        "id": "xchange-23/orders_to_ingest-2023_05_16-19_09_21-282726.zip/1684256975277223",
        "selfLink": "https://www.googleapis.com/storage/v1/b/xchange-23/o/orders_to_ingest-2023_05_16-19_09_21-282726.zip",
        "name": "orders_to_ingest-2023_05_16-19_09_21-282726.zip",
        "bucket": "xchange-23",
        "generation": "1684256975277223",
        "metageneration": "1",
        "contentType": "application/octet-stream",
        "timeCreated": "2023-05-16T17:09:35.315Z",
        "updated": "2023-05-16T17:09:35.315Z",
        "storageClass": "STANDARD",
        "timeStorageClassUpdated": "2023-05-16T17:09:35.315Z",
        "size": "647167",
        "md5Hash": "Gr/7UMe3BmkliIzIwlUB7Q==",
        "mediaLink": "https://storage.googleapis.com/download/storage/v1/b/xchange-23/o/orders_to_ingest-2023_05_16-19_09_21-282726.zip?generation=1684256975277223&alt=media",
        "crc32c": "MqZNtg==",
        "etag": "CKeZ45Wq+v4CEAE="
    }

    from main import ingest_data

    ingest_data(cloud_event)


if __name__ == "__main__":
    test_ingest_data()
