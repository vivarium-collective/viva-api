#!/usr/bin/env bash

set -eu  # Exit on error

# Get the directory where this script is located and calculate repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"

# Set paths relative to the repository root
NAMESPACE=sms-api-stanford
AWS_REGION=us-gov-west-1
SCRIPTS_DIR="${REPO_ROOT}/kustomize/scripts"
SECRETS_DIR="${REPO_ROOT}/kustomize/overlays/${NAMESPACE}"
MIGRATION_DIR="${REPO_ROOT}/kustomize/overlays/${NAMESPACE}-db-migration"
# Sibling overlay (backlog item 21 split): vivarium-workbench's own
# independently-applicable kustomization. It needs its own copy of
# secret-ghcr.yaml (same pattern as MIGRATION_DIR above) since it doesn't
# reference secret-shared.yaml at all (workbench has no Postgres creds).
WORKBENCH_DIR="${REPO_ROOT}/kustomize/overlays/${NAMESPACE}-workbench"
CONFIG_DIR="${REPO_ROOT}/kustomize/config/${NAMESPACE}"

# Load secrets from data file (not committed to git)
SECRETS_DATA_FILE="${SECRETS_DIR}/secrets.dat"
if [ ! -f "$SECRETS_DATA_FILE" ]; then
    echo "ERROR: Secrets data file not found: $SECRETS_DATA_FILE"
    echo "Please create it from secrets.dat.template"
    echo "  cp ${SECRETS_DIR}/secrets.dat.template ${SECRETS_DIR}/secrets.dat"
    echo "  # Then edit secrets.dat with your actual values"
    exit 1
fi

echo "Loading secrets from: $SECRETS_DATA_FILE"
source "$SECRETS_DATA_FILE"

# Validate STACK_PREFIX is set
if [ -z "$STACK_PREFIX" ]; then
    echo "ERROR: STACK_PREFIX not set in $SECRETS_DATA_FILE"
    echo "Please add: STACK_PREFIX=\"your-stack-prefix\" (e.g., smscdk, smsvpctest)"
    exit 1
fi

echo "Stack prefix: $STACK_PREFIX"

# Helper function to get CloudFormation stack output
get_stack_output() {
    local stack_name="$1"
    local output_key="$2"
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='$output_key'].OutputValue" \
        --output text 2>/dev/null
}

# Look up resources from CloudFormation stack outputs
echo ""
echo "=== Looking up resources from CloudFormation stacks ==="

# Get database secret ARN from shared stack
SECRET_ARN=$(get_stack_output "${STACK_PREFIX}-shared" "DbSecretArn")
if [ -z "$SECRET_ARN" ] || [ "$SECRET_ARN" = "None" ]; then
    echo "ERROR: Could not find DbSecretArn from ${STACK_PREFIX}-shared stack"
    exit 1
fi
echo "✓ Database secret ARN: $SECRET_ARN"

# Retrieve database credentials from AWS Secrets Manager
SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id $SECRET_ARN --region $AWS_REGION --query SecretString --output text)

POSTGRES_USER=$(echo $SECRET_JSON | jq -r '.username')
POSTGRES_PASSWORD=$(echo $SECRET_JSON | jq -r '.password')
POSTGRES_HOST=$(echo $SECRET_JSON | jq -r '.host')
POSTGRES_PORT=$(echo $SECRET_JSON | jq -r '.port')
POSTGRES_DATABASE=postgres

# Generate sealed secrets
echo ""
echo "=== Generating Sealed Secrets ==="

# Fetch sealed-secrets certificate from cluster (needed for AWS GovCloud)
echo "Fetching sealed-secrets certificate from cluster..."
CERT_FILE=$(mktemp)
kubectl get secret -n kube-system -l sealedsecrets.bitnami.com/sealed-secrets-key -o jsonpath='{.items[0].data.tls\.crt}' | base64 -d > "${CERT_FILE}"
echo "✓ Certificate saved to temporary file"

# Cleanup function to remove temp cert file
cleanup() {
    rm -f "${CERT_FILE}"
}
trap cleanup EXIT

# call sealed_secret_shared.sh <namespace> <db_password> <jms_password> <mongo_user> <mongo_pswd>
echo "Generating shared secrets..."
${SCRIPTS_DIR}/sealed_secret_shared.sh --cert "${CERT_FILE}" --controller-name sealed-secrets --controller-namespace kube-system ${NAMESPACE} ${POSTGRES_USER} ${POSTGRES_PASSWORD} ${POSTGRES_DATABASE} ${POSTGRES_HOST} ${POSTGRES_PORT} > ${SECRETS_DIR}/secret-shared.yaml
cp ${SECRETS_DIR}/secret-shared.yaml ${MIGRATION_DIR}/secret-shared.yaml
echo "✓ secret-shared.yaml generated"

# call sealed_secret_ghcr.sh <namespace> <github_user> <github_user_email> <github_token>
echo "Generating GHCR secrets..."
${SCRIPTS_DIR}/sealed_secret_ghcr.sh --cert "${CERT_FILE}" --controller-name sealed-secrets --controller-namespace kube-system ${NAMESPACE} ${GH_USER_NAME} ${GH_USER_EMAIL} ${GH_PAT} > ${SECRETS_DIR}/secret-ghcr.yaml
cp ${SECRETS_DIR}/secret-ghcr.yaml ${MIGRATION_DIR}/secret-ghcr.yaml
cp ${SECRETS_DIR}/secret-ghcr.yaml ${WORKBENCH_DIR}/secret-ghcr.yaml
echo "✓ secret-ghcr.yaml generated"

echo ""
echo "=== Updating Redis Configuration in shared.env ==="

# Get ElastiCache Redis endpoint from CloudFormation stack outputs
echo "Retrieving ElastiCache Redis endpoint from ${STACK_PREFIX}-shared stack..."
REDIS_ENDPOINT=$(get_stack_output "${STACK_PREFIX}-shared" "RedisEndpoint")

if [ -z "$REDIS_ENDPOINT" ] || [ "$REDIS_ENDPOINT" = "None" ]; then
    echo "ERROR: Could not find RedisEndpoint from ${STACK_PREFIX}-shared stack"
    exit 1
fi

echo "✓ Redis endpoint: ${REDIS_ENDPOINT}"

# Update shared.env with the Redis endpoint
SHARED_ENV_FILE="${CONFIG_DIR}/shared.env"
echo "Updating Redis hosts in ${SHARED_ENV_FILE}..."

sed -i.bak \
  -e "s|^REDIS_INTERNAL_HOST=.*|REDIS_INTERNAL_HOST=${REDIS_ENDPOINT}|" \
  -e "s|^REDIS_EXTERNAL_HOST=.*|REDIS_EXTERNAL_HOST=${REDIS_ENDPOINT}|" \
  "${SHARED_ENV_FILE}" && rm -f "${SHARED_ENV_FILE}.bak"

echo "✓ Redis configuration updated in shared.env"

echo ""
echo "=== Updating S3 Bucket in shared.env ==="

S3_BUCKET=$(get_stack_output "${STACK_PREFIX}-shared" "SharedBucketName")
if [ -z "$S3_BUCKET" ] || [ "$S3_BUCKET" = "None" ]; then
    echo "ERROR: Could not find SharedBucketName from ${STACK_PREFIX}-shared stack"
    exit 1
fi
echo "✓ S3 bucket: ${S3_BUCKET}"

sed -i.bak \
  -e "s|^S3_WORK_BUCKET=.*|S3_WORK_BUCKET=${S3_BUCKET}|" \
  -e "s|^STORAGE_S3_BUCKET=.*|STORAGE_S3_BUCKET=${S3_BUCKET}|" \
  "${SHARED_ENV_FILE}" && rm -f "${SHARED_ENV_FILE}.bak"

echo "✓ S3 bucket updated in shared.env"

echo ""
echo "=== Updating Batch Queue Names in shared.env ==="

# Look up queue names from CloudFormation stack outputs so they stay in sync
# with the CDK-managed Batch infrastructure (queues are prefixed with STACK_PREFIX).
echo "Retrieving Batch queue names from ${STACK_PREFIX}-batch and ${STACK_PREFIX}-build-batch stacks..."

BATCH_AMD64=$(get_stack_output "${STACK_PREFIX}-batch" "Amd64TaskQueueName")
BATCH_ARM64=$(get_stack_output "${STACK_PREFIX}-batch" "Arm64TaskQueueName")
BUILD_AMD64=$(get_stack_output "${STACK_PREFIX}-build-batch" "Amd64BuildQueueName")
BUILD_ARM64=$(get_stack_output "${STACK_PREFIX}-build-batch" "Arm64BuildQueueName")
BUILD_JOB_DEF=$(get_stack_output "${STACK_PREFIX}-build-batch" "DindBuildJobDefinitionName")
BUILD_GIT_SECRET=$(get_stack_output "${STACK_PREFIX}-build-batch" "GitSecretArn")

for var_name in BATCH_AMD64 BATCH_ARM64 BUILD_AMD64 BUILD_ARM64 BUILD_JOB_DEF BUILD_GIT_SECRET; do
    val="${!var_name}"
    if [ -z "$val" ] || [ "$val" = "None" ]; then
        echo "ERROR: Could not resolve ${var_name} from CloudFormation outputs"
        exit 1
    fi
done

echo "✓ BATCH_AMD64_QUEUE: ${BATCH_AMD64}"
echo "✓ BATCH_ARM64_QUEUE: ${BATCH_ARM64}"
echo "✓ BUILD_AMD64_QUEUE: ${BUILD_AMD64}"
echo "✓ BUILD_ARM64_QUEUE: ${BUILD_ARM64}"
echo "✓ BUILD_JOB_DEFINITION: ${BUILD_JOB_DEF}"
echo "✓ BUILD_GIT_SECRET_ARN: ${BUILD_GIT_SECRET}"

sed -i.bak \
  -e "s|^BATCH_AMD64_QUEUE=.*|BATCH_AMD64_QUEUE=${BATCH_AMD64}|" \
  -e "s|^BATCH_ARM64_QUEUE=.*|BATCH_ARM64_QUEUE=${BATCH_ARM64}|" \
  -e "s|^BUILD_AMD64_QUEUE=.*|BUILD_AMD64_QUEUE=${BUILD_AMD64}|" \
  -e "s|^BUILD_ARM64_QUEUE=.*|BUILD_ARM64_QUEUE=${BUILD_ARM64}|" \
  -e "s|^BUILD_JOB_DEFINITION=.*|BUILD_JOB_DEFINITION=${BUILD_JOB_DEF}|" \
  -e "s|^BUILD_GIT_SECRET_ARN=.*|BUILD_GIT_SECRET_ARN=${BUILD_GIT_SECRET}|" \
  "${SHARED_ENV_FILE}" && rm -f "${SHARED_ENV_FILE}.bak"

echo "✓ Batch queue names updated in shared.env"

echo ""
echo "=== Updating Ray-on-Batch settings in shared.env (best-effort) ==="

# The Ray stacks (<prefix>-ray-batch, <prefix>-ray) are optional; only re-resolve if present.
RAY_MNP_QUEUE_VAL=$(get_stack_output "${STACK_PREFIX}-ray-batch" "RayMnpQueueName")
RAY_MNP_JOBDEF_VAL=$(get_stack_output "${STACK_PREFIX}-ray-batch" "RayMnpJobDefinition")
RAY_LOG_PREFIX_VAL=$(get_stack_output "${STACK_PREFIX}-ray" "RayLogS3Prefix")

if [ -n "$RAY_MNP_QUEUE_VAL" ] && [ "$RAY_MNP_QUEUE_VAL" != "None" ]; then
    echo "✓ RAY_MNP_QUEUE: ${RAY_MNP_QUEUE_VAL}"
    echo "✓ RAY_MNP_JOB_DEFINITION: ${RAY_MNP_JOBDEF_VAL}"
    sed -i.bak \
      -e "s|^RAY_MNP_QUEUE=.*|RAY_MNP_QUEUE=${RAY_MNP_QUEUE_VAL}|" \
      -e "s|^RAY_MNP_JOB_DEFINITION=.*|RAY_MNP_JOB_DEFINITION=${RAY_MNP_JOBDEF_VAL}|" \
      "${SHARED_ENV_FILE}" && rm -f "${SHARED_ENV_FILE}.bak"
    if [ -n "$RAY_LOG_PREFIX_VAL" ] && [ "$RAY_LOG_PREFIX_VAL" != "None" ]; then
        echo "✓ RAY_LOG_S3_PREFIX: ${RAY_LOG_PREFIX_VAL}"
        ESCAPED_RAY_LOG=$(printf '%s\n' "$RAY_LOG_PREFIX_VAL" | sed 's/[&/\]/\\&/g')
        sed -i.bak -e "s|^RAY_LOG_S3_PREFIX=.*|RAY_LOG_S3_PREFIX=${ESCAPED_RAY_LOG}|" \
          "${SHARED_ENV_FILE}" && rm -f "${SHARED_ENV_FILE}.bak"
    fi
    echo "✓ Ray settings updated in shared.env"
else
    echo "  (Ray stacks not found for ${STACK_PREFIX}; keeping the hardcoded RAY_* values)"
fi

echo ""
echo "=== Updating IRSA Role ARN in kustomization.yaml ==="

BATCH_SUBMIT_ROLE_ARN=$(get_stack_output "${STACK_PREFIX}-batch" "BatchSubmitRoleArn")
if [ -z "$BATCH_SUBMIT_ROLE_ARN" ] || [ "$BATCH_SUBMIT_ROLE_ARN" = "None" ]; then
    echo "ERROR: Could not find BatchSubmitRoleArn from ${STACK_PREFIX}-batch stack"
    exit 1
fi
echo "✓ BatchSubmitRoleArn: ${BATCH_SUBMIT_ROLE_ARN}"

KUSTOMIZATION_FILE="${SECRETS_DIR}/kustomization.yaml"
# Replace any existing IRSA ARN or the placeholder
sed -i.bak \
  -e "s|value: .*BATCH_SUBMIT_IRSA_ROLE_ARN.*|value: ${BATCH_SUBMIT_ROLE_ARN}|" \
  -e "s|value: arn:aws-us-gov:iam:.*BatchSubmitIrsa.*|value: ${BATCH_SUBMIT_ROLE_ARN}|" \
  "${KUSTOMIZATION_FILE}" && rm -f "${KUSTOMIZATION_FILE}.bak"

echo "✓ IRSA role ARN updated in kustomization.yaml"

echo ""
echo "=== Updating Target Group Bindings for Verified Access ==="

# Get Target Group ARNs from CDK stack outputs
echo "Retrieving Target Group ARNs from ${STACK_PREFIX}-internal-alb stack..."
API_TARGET_GROUP_ARN=$(get_stack_output "${STACK_PREFIX}-internal-alb" "ApiTargetGroupArn")
PTOOLS_TARGET_GROUP_ARN=$(get_stack_output "${STACK_PREFIX}-internal-alb" "PtoolsTargetGroupArn")

if [ -z "$API_TARGET_GROUP_ARN" ] || [ "$API_TARGET_GROUP_ARN" = "None" ]; then
    echo "ERROR: Could not find ApiTargetGroupArn from ${STACK_PREFIX}-internal-alb stack"
    exit 1
fi

if [ -z "$PTOOLS_TARGET_GROUP_ARN" ] || [ "$PTOOLS_TARGET_GROUP_ARN" = "None" ]; then
    echo "ERROR: Could not find PtoolsTargetGroupArn from ${STACK_PREFIX}-internal-alb stack"
    exit 1
fi

echo "✓ API Target Group ARN: ${API_TARGET_GROUP_ARN}"
echo "✓ Ptools Target Group ARN: ${PTOOLS_TARGET_GROUP_ARN}"

# Generate the TargetGroupBinding YAML from template
TGB_TEMPLATE="${SECRETS_DIR}/target-group-binding.yaml.template"
TGB_FILE="${SECRETS_DIR}/target-group-binding.yaml"
echo "Generating TargetGroupBinding YAML from template..."

sed \
  -e "s|\${API_TARGET_GROUP_ARN}|${API_TARGET_GROUP_ARN}|g" \
  -e "s|\${PTOOLS_TARGET_GROUP_ARN}|${PTOOLS_TARGET_GROUP_ARN}|g" \
  "${TGB_TEMPLATE}" > "${TGB_FILE}"

echo "✓ TargetGroupBinding YAML generated: ${TGB_FILE}"

echo ""
echo "=== Uploading GitHub PAT for DooD Builds to Secrets Manager ==="

# Get the git secret ARN from the build-batch stack
GIT_SECRET_ARN=$(get_stack_output "${STACK_PREFIX}-build-batch" "GitSecretArn")

if [ -z "$GIT_SECRET_ARN" ] || [ "$GIT_SECRET_ARN" = "None" ]; then
    echo "WARNING: Could not find GitSecretArn from ${STACK_PREFIX}-build-batch stack, skipping"
elif [ -z "${GITHUB_BUILD_PAT:-}" ]; then
    echo "WARNING: GITHUB_BUILD_PAT not set in secrets.dat, skipping"
else
    # Allow reusing the GH_PAT used for GHCR
    BUILD_PAT="${GITHUB_BUILD_PAT}"
    if [ "$BUILD_PAT" = "use_gh_pat" ]; then
        BUILD_PAT="${GH_PAT}"
    fi
    ${SCRIPTS_DIR}/upload_deploy_key.sh "$GIT_SECRET_ARN" "$BUILD_PAT" "$AWS_REGION"
fi

echo ""
echo "=== All secrets and configuration files generated successfully! ==="
