# GHCR Tokens and GitHub Actions Deployment

This guide explains how to create two GitHub Container Registry (GHCR) tokens:

1. A **deployment token** used by GitHub Actions workflows to push images.
2. A **read-only token** that can be checked into the repository so anyone can pull the image.

## 1. Creating a Deployment Token

1. Log into GitHub and go to **Settings → Developer settings → Personal access tokens → Fine-grained tokens**.
2. Click **Generate new token**.
3. Give the token a name (for example `ghcr-deploy`), select your repository, and grant the `packages:read` and `packages:write` permissions.
4. Generate the token and copy it somewhere safe.

Store this token in your repository under **Settings → Secrets and variables → Actions** with the name `GHCR_TOKEN`. Store your GitHub username in a secret named `GHCR_USERNAME`.

The workflow in `.github/workflows/publish-integration-test-image.yml` uses these secrets to authenticate and push the image to GHCR:

```yaml
- name: Log in to GitHub Container Registry
  uses: docker/login-action@v3
  with:
    registry: ghcr.io
    username: ${{ secrets.GHCR_USERNAME }}
    password: ${{ secrets.GHCR_TOKEN }}
```

With `workflow_dispatch:` at the top of the file, you can trigger the workflow from the Actions tab whenever you're ready to publish.

## 2. Creating a Read‑Only Token

1. Follow the same steps as above to generate a token, but select only the `read:packages` scope.
2. Save this token in a text file within the repository, for example `resources/ghcr-read-token.txt`, so that collaborators can pull the image.

To use the read‑only token locally:

```bash
docker login ghcr.io -u <your-username> -p <read-only-token>
```

Then pull the container image:

```bash
docker pull ghcr.io/<owner>/flink-dotnet-windows:latest
```

---
[Home](https://github.com/devstress/FLINK.NET/blob/main/docs/wiki/Wiki-Structure-Outline.md)
