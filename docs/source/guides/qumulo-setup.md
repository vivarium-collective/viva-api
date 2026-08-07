# Qumulo S3 Storage Setup Guide

## Overview

This guide explains how to configure and test the Qumulo S3-compatible FileService implementation.

## Network Requirements

⚠️ **Important**: The Qumulo server (`cfs15.cam.uchc.edu:9000`) is on a private network and requires:

- **VPN connection** to the UCHC network, OR
- **Direct connection** to the campus network

## Configuration

The Qumulo settings are already configured in `assets/dev/config/.dev_env`:

```bash
# Qumulo S3-compatible storage settings
STORAGE_QUMULO_ENDPOINT_URL=https://cfs15.cam.uchc.edu:9000
STORAGE_QUMULO_BUCKET=sms-vivarium
STORAGE_QUMULO_ACCESS_KEY_ID=<<access-key-id>>
STORAGE_QUMULO_SECRET_ACCESS_KEY=<<secret-access-key>>
STORAGE_QUMULO_VERIFY_SSL=false
```

### Key Configuration Notes:

1. **Endpoint URL**: `https://cfs15.cam.uchc.edu:9000`
   - **Uses HTTPS** (not HTTP) - the server requires TLS/SSL
   - Port 9000 is the standard S3-compatible API port
   - Path-based bucket access

2. **Bucket**: `sms-vivarium`
   - Path-based (not virtual-hosted style)
   - Filesystem-oriented storage

3. **SSL Verification**: `false`
   - Set to false because the server uses a self-signed certificate
   - The connection is encrypted (HTTPS) but certificate verification is disabled
   - In production, you may want to configure proper SSL certificates

4. **Checksum Compatibility**: Qumulo doesn't support AWS's newer checksums
   - The FileService automatically disables CRC64NVME and other newer checksums
   - Sets `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` environment variable
   - This is required for write operations to succeed

## Testing Qumulo Connection

### Step 1: Verify Network Connectivity

```bash
# Test if you can reach the Qumulo server
ping cfs15.cam.uchc.edu

# Test if port 9000 is accessible
nc -zv cfs15.cam.uchc.edu 9000
# OR
telnet cfs15.cam.uchc.edu 9000
```

If these fail, you need to:
- Connect to UCHC VPN
- Ensure you're on the campus network

### Step 2: Test with AWS CLI

Once connected to the network:

```bash
# Set credentials as environment variables
export QUMULO_ACCESS_KEY="<<access-key-id>>"
export QUMULO_SECRET="<<secret-key>>"

# List bucket contents
AWS_ACCESS_KEY_ID=$QUMULO_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=$QUMULO_SECRET \
aws s3 ls s3://sms-vivarium/ \
  --endpoint-url https://cfs15.cam.uchc.edu:9000 \
  --no-verify-ssl

# Upload a test file (requires checksum settings for Qumulo compatibility)
echo "Test content" > /tmp/test.txt
AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
AWS_S3_ADDRESSING_STYLE=path \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=$QUMULO_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=$QUMULO_SECRET \
aws s3api put-object \
  --bucket sms-vivarium \
  --key test/aws_test.txt \
  --body /tmp/test.txt \
  --endpoint-url https://cfs15.cam.uchc.edu:9000 \
  --no-verify-ssl

# Download the file
AWS_ACCESS_KEY_ID=$QUMULO_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=$QUMULO_SECRET \
aws s3 cp s3://sms-vivarium/test/aws_test.txt /tmp/downloaded.txt \
  --endpoint-url https://cfs15.cam.uchc.edu:9000 \
  --no-verify-ssl

# Verify content
cat /tmp/downloaded.txt
```

### Step 3: Run Python Tests

```bash
# Switch to Qumulo backend
# Edit assets/dev/config/.dev_env:
STORAGE_BACKEND=qumulo

# Run the Qumulo integration tests
uv run pytest tests/test_qumulo_s3.py -v -s

# Or run a specific test
uv run pytest tests/test_qumulo_s3.py::test_qumulo_file_service -v -s
```

## Using Qumulo FileService in Your Code

### Option 1: Via Configured Backend (Recommended)

```python
from viva_api.dependencies import get_file_service

# Get the configured file service (Qumulo when STORAGE_BACKEND=qumulo)
file_service = get_file_service()

# Upload
await file_service.upload_file(
    Path("local/file.txt"),
    "simulations/results/data.txt"  # Path-based, maps to Qumulo filesystem
)

# Download
contents = await file_service.get_file_contents("simulations/results/data.txt")
```

### Option 2: Direct Instantiation

```python
from viva_api.common.storage import FileServiceQumuloS3

# Create Qumulo service
qumulo = FileServiceQumuloS3()

try:
    # Upload bytes
    await qumulo.upload_bytes(
        b"simulation data",
        "experiments/exp001/output.bin"
    )

    # List files
    listing = await qumulo.get_listing("experiments/exp001/")
    for item in listing:
        print(f"{item.Key}: {item.Size} bytes")

finally:
    await qumulo.close()
```

### Option 3: In Tests (with Fixture)

```python
import pytest
from viva_api.common.storage import FileServiceQumuloS3

@pytest.mark.asyncio
async def test_my_feature(file_service_qumulo: FileServiceQumuloS3):
    # Service is already configured and injected!
    result = await file_service_qumulo.upload_bytes(
        b"test data",
        "test/my_test.bin"
    )
    # Automatic cleanup via fixture
```

## Path Format for Qumulo

Qumulo uses path-based bucket access. All these formats work:

```python
# Relative path (recommended)
"simulations/exp001/results.parquet"

# Absolute-style path
"/simulations/exp001/results.parquet"

# With protocol prefix
"qumulo://simulations/exp001/results.parquet"
```

The bucket (`sms-vivarium`) is automatically prepended, so the full S3 path becomes:
```
s3://sms-vivarium/simulations/exp001/results.parquet
```

## Troubleshooting

### Connection Timeouts

**Symptom**: Tests hang or timeout when connecting to Qumulo

**Solutions**:
1. Verify you're connected to UCHC VPN
2. Check network connectivity: `ping cfs15.cam.uchc.edu`
3. Verify port access: `nc -zv cfs15.cam.uchc.edu 9000`
4. Check firewall rules on your machine

### Authentication Errors

**Symptom**: `403 Forbidden` or `Access Denied`

**Solutions**:
1. Verify credentials in `.dev_env` are correct
2. Check that credentials haven't expired
3. Ensure the access key has permissions for the `sms-vivarium` bucket

### SSL/TLS Errors

**Symptom**: `SSL verification failed` or certificate errors

**Solutions**:
1. Ensure `STORAGE_QUMULO_VERIFY_SSL=false` in `.dev_env`
2. If Qumulo is using HTTPS, update endpoint to `https://` and set verify to `true`

### Wrong Region Errors

**Note**: Qumulo doesn't use AWS regions. The implementation ignores region settings.

## Comparison: AWS S3 vs Qumulo S3

| Feature | AWS S3 | Qumulo S3 |
|---------|--------|-----------|
| Endpoint | `s3.amazonaws.com` | `cfs15.cam.uchc.edu:9000` |
| Authentication | IAM, SSO, Keys | Access Key/Secret only |
| SSL/TLS | Required (HTTPS) | Optional (HTTP) |
| Regions | Required | N/A (ignored) |
| Bucket Style | Virtual-hosted | Path-based |
| Network | Public internet | Private network (VPN required) |

## Production Deployment

When deploying to Kubernetes/HPC environments that have network access to Qumulo:

```yaml
# ConfigMap or Secret
apiVersion: v1
kind: Secret
metadata:
  name: storage-config
stringData:
  STORAGE_BACKEND: "qumulo"
  STORAGE_QUMULO_ENDPOINT_URL: "http://cfs15.cam.uchc.edu:9000"
  STORAGE_QUMULO_BUCKET: "sms-vivarium"
  STORAGE_QUMULO_ACCESS_KEY_ID: "<your-key>"
  STORAGE_QUMULO_SECRET_ACCESS_KEY: "<your-secret>"
  STORAGE_QUMULO_VERIFY_SSL: "false"
```

## Next Steps

Once network connectivity is established:

1. Run basic connection tests with AWS CLI
2. Run Python integration tests
3. Upload/download test files
4. Integrate with your simulation workflows

## Support

If you encounter issues:
- Check network connectivity to `cfs15.cam.uchc.edu`
- Verify VPN connection to UCHC network
- Contact UCHC IT for Qumulo access issues
- Check Qumulo documentation for S3 API compatibility
