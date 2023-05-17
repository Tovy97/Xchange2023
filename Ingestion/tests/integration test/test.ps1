.\venv\Scripts\activate
$env:PROJECT_ID='goreply-xchange2023-datastudio'
$env:SECRET_ID='csv_file_decryption_password'
$env:ARCHIVE_BUCKET='xchange-23_archive'
$env:ARCHIVE='False'
functions-framework --target=ingest_data --signature-type=cloudevent --debug