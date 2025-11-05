import { Container, getRandom } from "@cloudflare/containers";
import { Hono } from "hono";

const INSTANCE_COUNT = 3;

export class MiniRuntimeContainerNginx extends Container {
  // Port the container listens on (default: 8080)
  defaultPort = 8080;
  // Time before container sleeps due to inactivity (default: 30s)
  sleepAfter = "2h";
  
  // Environment variables passed to the container
  envVars = {
    AKTO_LOG_LEVEL: "INFO",
    DATABASE_ABSTRACTOR_SERVICE_TOKEN: "<TOKEN>",
    DATABASE_ABSTRACTOR_SERVICE_URL: "https://cyborg.akto.io",
    AKTO_TRAFFIC_QUEUE_THRESHOLD: "100",
    AKTO_INACTIVE_QUEUE_PROCESSING_TIME: "5000",
    AKTO_TRAFFIC_PROCESSING_JOB_INTERVAL: "10",
    AKTO_CONFIG_NAME: "STAGING",
    RUNTIME_MODE: "HYBRID"
  };

  

  // Optional lifecycle hooks
  override onStart() {
    console.log("Mini-Runtime Container successfully started");
  }

  override onStop() {
    console.log("Mini-Runtime Container successfully shut down");
  }

  override onError(error: unknown) {
    console.log("Mini-Runtime Container error:", error);
  }

}

// Create Hono app with proper typing for Cloudflare Workers
type Environment = {
  readonly MINI_RUNTIME_CONTAINER_NGINX: DurableObjectNamespace<MiniRuntimeContainerNginx>
  readonly AKTO_TRAFFIC_QUEUE_NGINX: Queue<any>
}
const app = new Hono<{
  Bindings: Environment;
}>();

// Home route with available endpoints
app.get("/", (c) => {
  return c.text(
    "Akto Mini-Runtime Container is running!"
  );
});

export function normalizeBatchData(raw: any): { batchData: any[] } {
  try {
    // unwrap batchData if nested arrays exist
    let items = raw?.batchData ?? []

    // flatten nested arrays like [[{ body: "..." }]]
    items = items.flat(Infinity)

    // parse each item safely
    const normalized = items.map((item: any) => {
      try {
        if (typeof item === "string") {
          return JSON.parse(item)
        }
        if (item?.body && typeof item.body === "string") {
          return JSON.parse(item.body)
        }
        return item
      } catch (err) {
        console.error("Failed to parse item:", item, err)
        return item
      }
    })

    return { batchData: normalized }
  } catch (err) {
    console.error("Failed to normalize batch data:", raw, err)
    return { batchData: [] }
  }
}


export default {
  fetch: app.fetch,

  async queue(batch: MessageBatch<any>, env: Environment) {
    const messages = batch.messages

    // process in fixed-size slices sequentially
    for (let i = 0; i < messages.length; i += 5) {
      const batchSlice = messages.slice(i, i + 5)

      // prepare payload
      const payload = batchSlice.map((m) => m.body)
      const normalized = normalizeBatchData({ batchData: payload })
      const result = JSON.stringify(normalized)

      const containerInstance = getRandom(env.MINI_RUNTIME_CONTAINER_NGINX, INSTANCE_COUNT)
      const containerId = env.MINI_RUNTIME_CONTAINER_NGINX.idFromName(`/container/${containerInstance}`)
      const container = env.MINI_RUNTIME_CONTAINER_NGINX.get(containerId)

      const req = new Request("http://internal/upload", {
        method: "POST",
        body: result,
        headers: { "Content-Type": "application/json" },
      })

      try {
        // 🔒 sequentially wait for container response
        const res = await container.fetch(req)

        if (res.ok) {
          // 🔒 also sequentially ack messages
          for (const m of batchSlice) {
            await m.ack()
          }
          console.log(`Processed ${batchSlice.length} messages successfully`)
        } else {
          console.error(`Failed to process messages. Container status: ${res.status}`)

          // 🔒 requeue each message one by one before next batch
          for (const m of batchSlice) {
            await env.AKTO_TRAFFIC_QUEUE_NGINX.send(m.body)
          }
        }
      } catch (err) {
        console.error("Error sending messages to container:", err)

        for (const m of batchSlice) {
          await env.AKTO_TRAFFIC_QUEUE_NGINX.send(m.body)
        }
      }
    }
  },
}
