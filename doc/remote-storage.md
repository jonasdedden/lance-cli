# Remote object stores

Every command accepts an object-store URI wherever it accepts a local path.

```sh
lance-cli head -n 5 s3://my-bucket/datasets/embeddings.lance
lance-cli rowcount gs://analytics/events.lance
lance-cli schema az://container/data.lance
lance-cli versions s3://my-bucket/datasets/embeddings.lance
```

| Scheme | Backend | Credentials |
|---|---|---|
| `s3://` | AWS S3 and S3-compatible stores | Standard AWS SDK chain: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_REGION`, instance and role metadata. |
| `gs://` | Google Cloud Storage | `GOOGLE_APPLICATION_CREDENTIALS` (service-account JSON), or `gcloud` application-default credentials. |
| `az://` | Azure Blob Storage | `AZURE_STORAGE_ACCOUNT_NAME` plus `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_TOKEN`, and the other standard Azure variables. |
| `file://` | Local filesystem | none |
| *(none)* | Local filesystem | none |

Credentials come only from the ambient environment; there are no lance-cli-specific
credential flags. A bare path, relative or absolute, always resolves to the
local filesystem. Object-store errors such as missing credentials, 404, or
permission denied are reported with the offending URI and the underlying cause.

Two things worth knowing:

- `file://` URIs must be absolute (`file:///abs/path.lance`). A relative
  `file://path` resolves against the current directory in a confusing way; use
  a bare relative path instead.
- A `gs://` URI with no ambient credentials can stall for 90 to 100 seconds
  while `object_store` probes the GCE metadata server before failing. This is
  upstream behavior. `s3://` fails within seconds and `az://` fails
  immediately.
