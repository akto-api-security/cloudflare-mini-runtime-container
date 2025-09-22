# Cloudflare Mini Runtime Container

This project deploys the Akto mini-runtime container to Cloudflare Workers.

## Prerequisites

Before deploying, you need to:

1. Have a Cloudflare account with Workers enabled
2. Have `wrangler` CLI installed and authenticated
3. Access to your Akto dashboard for configuration values

## Deployment Steps

### 1. Create the Queue

First, create the required queue:

```bash
wrangler queues create akto-traffic-queue
```

### 2. Push the Container Image

Pull the Akto mini-runtime container:

```bash
docker pull --platform linux/amd64 aktosecurity/mini-runtime-container:latest
```

Push it to your Cloudflare registry:

```bash
wrangler containers push aktosecurity/mini-runtime-container:latest
```

### 3. Configure Container Image

After pushing, you'll receive a container image URL. Add this URL to the `image` field in your `wrangler.jsonc` file:

```jsonc
{
  "compatibility_date": "2023-05-18",
  "main": "src/index.ts",
  "container": {
    "image": "YOUR_PUSHED_IMAGE_URL_HERE"
  }
}
```

### 4. Configure Environment Variables

In your `index.ts` file, update the `envVars` object with your Akto configuration:

```typescript
const envVars = {
  AKTO_DATABASE_ABSTRACTOR_TOKEN: "YOUR_UNIQUE_TOKEN_FROM_AKTO_DASHBOARD",
  AKTO_DATABASE_ABSTRACTOR_URL: "https://cyborg.akto.io"
}
```

You can get your unique token from your Akto dashboard. The database abstractor service URL is `https://cyborg.akto.io` by default.

### 5. Deploy

Once configured, deploy your worker:

```bash
wrangler deploy
```

## Configuration Values

- **Database Abstractor Token**: Get this from your Akto dashboard (unique per user)
- **Database Abstractor URL**: Default is `https://cyborg.akto.io`
- **Container Image URL**: Generated when you push the container to Cloudflare registry