# 1. Variables for reusability (and easy environment switching)
PROJECT_ID="project-6e4dc205-0d7d-44c6-975"
MAIN_TOPIC="hyperion-telemetry-input"
DLQ_TOPIC="hyperion-dlq"
MAIN_SUB="hyperion-dataflow-sub"

# 2. Create the Topics
# The main highway for data
gcloud pubsub topics create $MAIN_TOPIC --project=$PROJECT_ID

# The parking lot for "bad" data (Dead Letter Queue)
gcloud pubsub topics create $DLQ_TOPIC --project=$PROJECT_ID

# 3. CRITICAL STEP: Grant Permissions
# The Pub/Sub Service Agent needs permission to move messages
# from the subscription to the DLQ. Without this, DLQ will fail silently.
PUBSUB_SERVICE_ACCOUNT="service-$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")@gcp-sa-pubsub.iam.gserviceaccount.com"

# Allow the service agent to publish to the DLQ
gcloud pubsub topics add-iam-policy-binding $DLQ_TOPIC \
    --member="serviceAccount:$PUBSUB_SERVICE_ACCOUNT" \
    --role="roles/pubsub.publisher"

# Allow the service agent to acknowledge messages in the subscription
gcloud pubsub subscriptions add-iam-policy-binding $MAIN_SUB \
    --member="serviceAccount:$PUBSUB_SERVICE_ACCOUNT" \
    --role="roles/pubsub.subscriber" \
    --condition=None 2>/dev/null || echo "Subscription doesn't exist yet, proceeding..."

# 4. Create the Subscription with DLQ Policy
# We set max-delivery-attempts to 5. If Dataflow fails to process
# a message 5 times, it gets moved to the DLQ so the pipeline doesn't get stuck.
gcloud pubsub subscriptions create $MAIN_SUB \
    --topic=$MAIN_TOPIC \
    --project=$PROJECT_ID \
    --dead-letter-topic=$DLQ_TOPIC \
    --max-delivery-attempts=5 \
    --ack-deadline=60 \
    --enable-message-ordering