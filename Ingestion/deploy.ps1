$FUNCTION_NAME = "ingest-data"
$ENTRY_POINT = "ingest_data"
$PROJECT = "goreply-xchange2023-datastudio"
$REGION = "europe-west1"
$MEMORY = "512MiB"
$RUNTIME = "python310"
$TIMEOUT = "540s"
$SOURCE = "."
$MAX_INSTANCE = 10
$MIN_INSTANCE = 0
$BUCKET_NAME = "xchange-23"
$FUNCTION_SA = "cloud-function-sa@goreply-xchange2023-datastudio.iam.gserviceaccount.com"
$EVENTARC_SA = "eventarc-trigger-sa@goreply-xchange2023-datastudio.iam.gserviceaccount.com"
$YAML_ENV_FILE = "env.yaml"
$DECRYPTION_PASSWORD = "DECRYPTION_PASSWORD=csv_file_decryption_password:latest"

gcloud functions deploy                     $FUNCTION_NAME `
    --project                               $PROJECT `
    --region                                $REGION `
    --no-allow-unauthenticated              `
    --entry-point                           $ENTRY_POINT `
    --gen2                                  `
    --memory                                $MEMORY `
    --retry                                 `
    --run-service-account                   $FUNCTION_SA `
    --service-account                       $FUNCTION_SA `
    --runtime                               $RUNTIME `
    --serve-all-traffic-latest-revision     `
    --source                                $SOURCE `
    --timeout                               $TIMEOUT `
    --trigger-location                      $REGION `
    --trigger-service-account               $EVENTARC_SA `
    --max-instances                         $MAX_INSTANCE `
    --min-instances                         $MIN_INSTANCE `
    --trigger-event                         google.cloud.storage.object.v1.finalized `
    --trigger-resource                      $BUCKET_NAME `
    --env-vars-file                         $YAML_ENV_FILE `
    --set-secrets                           $DECRYPTION_PASSWORD
