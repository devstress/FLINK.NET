# GHCR Public Image and GitHub Actions

This guide explains how to publish the integration test Docker image to GitHub Container Registry (GHCR) and make it publicly accessible.

## 1. Make the GHCR Package Public

1. After the image is pushed the first time, open the **Packages** section of your repository on GitHub.
2. Select the `flink-dotnet-linux` package.
3. Under **Package settings**, change the visibility to **Public**.

Once the package is public, anyone can pull the image without authentication.

## 2. Publishing with GitHub Actions

The workflow `.github/workflows/publish-integration-test-image.yml` uses GitHub's built-in `${{ secrets.GITHUB_TOKEN }}` to authenticate and automatically runs whenever changes are pushed to the `IntegrationTestImage` directory:

```yaml
- name: Log in to GitHub Container Registry
  uses: docker/login-action@v3
  with:
    registry: ghcr.io
    username: ${{ github.repository_owner }}
    password: ${{ secrets.GITHUB_TOKEN }}
```

You can also trigger the workflow manually from the Actions tab thanks to `workflow_dispatch:`.

---
[Home](./Wiki-Structure-Outline.md)
