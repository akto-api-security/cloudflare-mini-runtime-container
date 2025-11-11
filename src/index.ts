import { Container, getRandom } from "@cloudflare/containers";
import { Hono } from "hono";

const INSTANCE_COUNT = 3;

export class MiniRuntimeContainer extends Container {
  // Port the container listens on (default: 8080)
  defaultPort = 8080;
  // Time before container sleeps due to inactivity (default: 30s)
  sleepAfter = "2h";
  
  // Environment variables passed to the container
  envVars = {
    AKTO_LOG_LEVEL: "DEBUG",
    DATABASE_ABSTRACTOR_SERVICE_TOKEN: "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJBa3RvIiwic3ViIjoiaW52aXRlX3VzZXIiLCJhY2NvdW50SWQiOjE3MDAwODc5NjQsImlhdCI6MTc2MTYyNjYyMSwiZXhwIjoxNzc3MzUxNDIxfQ.Gjq7tCwHmfrUBEQMn5WBd9WQY2ey7m8UFbJlOh249xMp3ILsS3uzA5IAjcG_5RpTNBWcXa_6AURm4LlAzCm63DyxyJzh2ZacWTGvyzGL59hOIrPouxoliWbtErmhOlBQon-oZsY1ZhMmESlXyRI6aaT2MVZazXyr39mqvaV41TkY0YeJRl7Tf3UkNKkKHEUT2WH7a2-MtTvHQAX-OlugRcL2YpKaLqjTvsluJcuAAOFncEuOY5db88z--w9M_vkMMKhMuvRbnndn87yiy1NjmeOpn32GbOdJhSMQrSbVmslXykdB5OyxVKh2Mw9IKXjMXaD7oE2mRLrdUSwEFR4yhA",
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
  readonly MINI_RUNTIME_CONTAINER: DurableObjectNamespace<MiniRuntimeContainer>
  readonly AKTO_TRAFFIC_QUEUE: Queue<any>
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

    // Check if this is the dev queue
    if (batch.queue === "akto-traffic-queue-dev") {
      console.log("=== akto-traffic-queue-dev received ===")
      console.log("Number of messages:", messages.length)

      messages.forEach((msg, idx) => {
        console.log(`\n=== Traffic Record ${idx + 1} ===`)

        try {
          // msg.body is { body: "..." } from producer
          let data = msg.body

          // Extract the stringified traffic data
          if (data && typeof data === "object" && "body" in data && typeof data.body === "string") {
            data = JSON.parse(data.body)
          } else if (typeof data === "string") {
            data = JSON.parse(data)
          }

          console.log(JSON.stringify(data, null, 2))
        } catch (e) {
          console.log("Raw body:", JSON.stringify(msg.body))
          console.log("Parse error:", e)
        }
      })

      // Acknowledge all messages
      for (const m of messages) {
        m.ack()
      }
      return
    }

    // process in fixed-size slices sequentially
    for (let i = 0; i < messages.length; i += 5) {
      const batchSlice = messages.slice(i, i + 5)

      // prepare payload
      const payload = batchSlice.map((m) => m.body)
      const normalized = normalizeBatchData({ batchData: payload })
      const result = JSON.stringify(normalized)

      const containerInstance = getRandom(env.MINI_RUNTIME_CONTAINER, INSTANCE_COUNT)
      const containerId = env.MINI_RUNTIME_CONTAINER.idFromName(`/container/${containerInstance}`)
      const container = env.MINI_RUNTIME_CONTAINER.get(containerId)

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
            await env.AKTO_TRAFFIC_QUEUE.send(m.body)
          }
        }
      } catch (err) {
        console.error("Error sending messages to container:", err)

        for (const m of batchSlice) {
          await env.AKTO_TRAFFIC_QUEUE.send(m.body)
        }
      }
    }
  },
}
