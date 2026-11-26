# Security notes

## Admin authentication

Username-only login has been removed. Configure `ISTRAZIVAC_ADMIN_PASSWORD` in the hosting environment with at least 16 characters. The app fails closed if it is unset or too short. Do not commit secrets or tokens.

## GitHub token incident

A previously tracked Streamlit secrets file contained a GitHub token. The file has been removed from the current branch and is ignored going forward, but old Git history may still contain the value. Revoke and rotate that token immediately. Removing a file from the latest commit does not revoke credentials or erase historical blobs.

## Crawler

The crawler validates HTTP/HTTPS seed URLs, rejects embedded credentials, verifies TLS, deduplicates URLs, and limits each crawl to 100 pages by default. Review and enforce network egress restrictions before exposing the crawler publicly; URL validation alone is not an SSRF defense.
