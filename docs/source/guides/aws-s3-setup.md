# AWS S3 Setup for SMS API

This document describes how to set up an AWS S3 bucket for use with the SMS API, including proper permissions for SSO users and avoiding KMS encryption issues.

## Requirements

### S3 Bucket Configuration
- **No KMS encryption** (or proper KMS key permissions for your SSO role)
- **Standard S3 bucket** in your preferred region
- **Versioning** (optional but recommended)
- **Lifecycle policies** (optional, for automatic cleanup)

### IAM Permissions Required
Your SSO role needs the following S3 permissions:
- `s3:PutObject` - Upload files
- `s3:GetObject` - Download files (also covers head/metadata operations)
- `s3:DeleteObject` - Delete files
- `s3:ListBucket` - List bucket contents
- `s3:GetObjectAttributes` - Get object metadata

If using KMS encryption, you also need:
- `kms:GenerateDataKey` - Generate encryption keys for new objects
- `kms:Decrypt` - Decrypt existing objects

## Setup Instructions

### Step 1: Check Current Bucket Configuration

```bash
# Check if bucket exists
aws s3 ls s3://your-bucket-name/

# Check bucket encryption
aws s3api get-bucket-encryption --bucket your-bucket-name

# Check your current identity
aws sts get-caller-identity
```

### Step 2: Create a New Bucket (Without KMS)

```bash
# Set variables
export AWS_REGION="us-east-1"
export BUCKET_NAME="sms-api-storage-$(date +%Y%m%d)"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export SSO_ROLE_ARN="arn:aws:sts::${ACCOUNT_ID}:assumed-role/AWSReservedSSO_AdministratorAccess_0623eedd7adb8854/jcschaff_sso"

# Create bucket
aws s3api create-bucket \
  --bucket "$BUCKET_NAME" \
  --region "$AWS_REGION" \
  --create-bucket-configuration LocationConstraint="$AWS_REGION"

echo "✅ Created bucket: $BUCKET_NAME"
```

**Note**: For `us-east-1`, omit the `--create-bucket-configuration` parameter:
```bash
aws s3api create-bucket \
  --bucket "$BUCKET_NAME" \
  --region us-east-1
```

### Step 3: Configure Bucket Encryption (SSE-S3, not KMS)

Use S3-managed encryption (SSE-S3) instead of KMS to avoid permission issues:

```bash
aws s3api put-bucket-encryption \
  --bucket "$BUCKET_NAME" \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        },
        "BucketKeyEnabled": false
      }
    ]
  }'

echo "✅ Configured SSE-S3 encryption (no KMS required)"
```

### Step 4: Enable Versioning (Optional but Recommended)

```bash
aws s3api put-bucket-versioning \
  --bucket "$BUCKET_NAME" \
  --versioning-configuration Status=Enabled

echo "✅ Enabled versioning"
```

### Step 5: Set Bucket Policy for SSO Access

Create a bucket policy that explicitly allows your SSO role:

```bash
cat > /tmp/bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSSOUserFullAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${SSO_ROLE_ARN}"
      },
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetObjectAttributes"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    }
  ]
}
EOF

aws s3api put-bucket-policy \
  --bucket "$BUCKET_NAME" \
  --policy file:///tmp/bucket-policy.json

echo "✅ Set bucket policy for SSO access"
```

### Step 6: Configure Lifecycle Policy (Optional)

Automatically delete test files after 7 days:

```bash
cat > /tmp/lifecycle-policy.json <<EOF
{
  "Rules": [
    {
      "Id": "DeleteTestFilesAfter7Days",
      "Status": "Enabled",
      "Prefix": "test/",
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET_NAME" \
  --lifecycle-configuration file:///tmp/lifecycle-policy.json

echo "✅ Configured lifecycle policy"
```

### Step 7: Test Bucket Access

```bash
# Test upload
echo "Test content" > /tmp/test-file.txt
aws s3 cp /tmp/test-file.txt "s3://${BUCKET_NAME}/test/upload-test.txt"

# Test download
aws s3 cp "s3://${BUCKET_NAME}/test/upload-test.txt" /tmp/test-download.txt

# Test delete
aws s3 rm "s3://${BUCKET_NAME}/test/upload-test.txt"

# Cleanup
rm /tmp/test-file.txt /tmp/test-download.txt

echo "✅ All operations succeeded!"
```

### Step 8: Update Environment Configuration

Add to your `.env` file or environment:

```bash
export STORAGE_S3_BUCKET="${BUCKET_NAME}"
export STORAGE_S3_REGION="${AWS_REGION}"
# AWS credentials are typically auto-configured via SSO
```

## Troubleshooting

### Error: `kms:GenerateDataKey` Permission Denied

This means your bucket has KMS encryption enabled but your SSO role lacks KMS permissions.

**Solution 1: Use SSE-S3 instead** (recommended, see Step 3 above)

**Solution 2: Add KMS permissions to your role**
```bash
# Check the KMS key being used
aws s3api get-bucket-encryption --bucket "$BUCKET_NAME"

# Your AWS administrator needs to add this to your SSO role policy:
{
  "Effect": "Allow",
  "Action": [
    "kms:GenerateDataKey",
    "kms:Decrypt"
  ],
  "Resource": "arn:aws:kms:REGION:ACCOUNT:key/KEY-ID"
}
```

### Error: `AccessDenied` on Upload

Check bucket policy and IAM permissions:
```bash
# View current bucket policy
aws s3api get-bucket-policy --bucket "$BUCKET_NAME" | jq -r '.Policy | fromjson'

# Check your identity
aws sts get-caller-identity
```

### Error: Bucket Already Exists

Bucket names are globally unique. Choose a different name:
```bash
export BUCKET_NAME="sms-api-storage-$(uuidgen | head -c 8 | tr '[:upper:]' '[:lower:]')"
```

## Common AWS CLI Operations

### Upload File
```bash
aws s3 cp local-file.txt s3://${BUCKET_NAME}/path/to/file.txt
```

### Upload with Metadata
```bash
aws s3 cp local-file.txt s3://${BUCKET_NAME}/path/to/file.txt \
  --metadata key1=value1,key2=value2
```

### Download File
```bash
aws s3 cp s3://${BUCKET_NAME}/path/to/file.txt local-file.txt
```

### List Files
```bash
# List all files
aws s3 ls s3://${BUCKET_NAME}/ --recursive

# List files with prefix
aws s3 ls s3://${BUCKET_NAME}/test/ --recursive
```

### Delete File
```bash
aws s3 rm s3://${BUCKET_NAME}/path/to/file.txt
```

### Delete All Files in Prefix
```bash
aws s3 rm s3://${BUCKET_NAME}/test/ --recursive
```

### Sync Directory to S3
```bash
aws s3 sync local-directory/ s3://${BUCKET_NAME}/remote-directory/
```

### Get Object Metadata
```bash
aws s3api head-object \
  --bucket "$BUCKET_NAME" \
  --key path/to/file.txt
```

### Check if Object Exists
```bash
if aws s3api head-object --bucket "$BUCKET_NAME" --key path/to/file.txt 2>/dev/null; then
  echo "File exists"
else
  echo "File does not exist"
fi
```

## Python API Usage

### Using FileServiceS3

```python
from viva_api.common.storage import FileServiceS3

# Initialize service (uses credentials from environment)
s3_service = FileServiceS3()

# Upload bytes
await s3_service.upload_bytes(
    file_contents=b"Hello, world!",
    gcs_path="test/hello.txt"
)

# Upload file
from pathlib import Path
await s3_service.upload_file(
    file_path=Path("local-file.txt"),
    gcs_path="test/uploaded-file.txt"
)

# Download file
gcs_path, local_path = await s3_service.download_file(
    gcs_path="test/hello.txt",
    file_path=Path("downloaded-file.txt")
)

# Get file contents
contents = await s3_service.get_file_contents("test/hello.txt")
print(contents.decode("utf-8"))

# List files
from viva_api.common.storage import ListingItem
files: list[ListingItem] = await s3_service.get_listing("test/")
for file in files:
    print(f"{file.Key} - {file.Size} bytes - {file.LastModified}")

# Delete file
await s3_service.delete_file("test/hello.txt")

# Close service
await s3_service.close()
```

## Security Best Practices

1. **Use SSE-S3 encryption** instead of KMS unless you have specific compliance requirements
2. **Enable bucket versioning** to protect against accidental deletions
3. **Use lifecycle policies** to automatically clean up old test files
4. **Use IAM roles** (SSO or EC2 instance roles) instead of hardcoded credentials
5. **Enable CloudTrail logging** for audit trails
6. **Use bucket policies** to restrict access to specific principals
7. **Enable S3 Block Public Access** to prevent accidental public exposure

## Configuration in SMS API

### Environment Variables

```bash
# Required
export STORAGE_S3_BUCKET="your-bucket-name"
export STORAGE_S3_REGION="us-east-1"

# Optional (if not using SSO/instance role)
export STORAGE_S3_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export STORAGE_S3_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export STORAGE_S3_SESSION_TOKEN="mysessiontoken"
```

### Settings (config.py)

```python
from viva_api.config import get_settings

settings = get_settings()
print(f"S3 Bucket: {settings.storage_s3_bucket}")
print(f"S3 Region: {settings.storage_s3_region}")
```

## Comparison with Qumulo

| Feature | AWS S3 | Qumulo S3 |
|---------|--------|-----------|
| Overwrites | ✅ Allowed by default | ❌ Blocked (requires delete-first) |
| Encryption | SSE-S3, SSE-KMS, SSE-C | Compatible with S3 API |
| Endpoint | Standard AWS endpoints | Custom endpoint URL |
| Authentication | AWS credentials/SSO | S3-compatible keys |
| Checksums | Full AWS checksum support | Limited (when_required mode) |
| SSL | Required (standard) | Optional (--no-verify-ssl) |

## References

- [AWS S3 API Documentation](https://docs.aws.amazon.com/s3/index.html)
- [AWS CLI S3 Commands](https://docs.aws.amazon.com/cli/latest/reference/s3/)
- [AWS S3 Encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html)
- [AWS S3 IAM Policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-policy-language-overview.html)
