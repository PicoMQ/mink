import { expect, type Page } from '@playwright/test'
import { Kafka, logLevel } from 'kafkajs'

export const ADMIN = process.env.MINK_ADMIN ?? 'http://127.0.0.1:9180'
const KAFKA = process.env.MINK_KAFKA ?? 'localhost:29092'
const REGISTRY = process.env.MINK_REGISTRY ?? 'http://localhost:8081'

const kafka = new Kafka({ clientId: 'mink-console-e2e', brokers: KAFKA.split(','), logLevel: logLevel.NOTHING })

export const SCHEMA = {
  type: 'object',
  properties: {
    id: { type: 'integer' },
    item: { type: 'string' },
  },
}

export function unique(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`
}

export async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const response = await fetch(`${ADMIN}${path}`, {
    method,
    headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok) {
    throw new Error(`${method} ${path}: ${response.status} ${await response.text()}`)
  }

  const text = await response.text()
  return (text === '' ? undefined : JSON.parse(text)) as T
}

export async function createTopic(name: string, partitions: number) {
  const admin = kafka.admin()
  await admin.connect()

  try {
    await admin.createTopics({ topics: [{ topic: name, numPartitions: partitions }], waitForLeaders: true })
  } finally {
    await admin.disconnect()
  }
}

export async function registerSchema(subject: string, schema: object): Promise<number> {
  const response = await fetch(`${REGISTRY}/subjects/${encodeURIComponent(subject)}/versions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
    body: JSON.stringify({ schemaType: 'JSON', schema: JSON.stringify(schema) }),
  })
  if (!response.ok) {
    throw new Error(`registering ${subject}: ${response.status} ${await response.text()}`)
  }

  const registered = (await response.json()) as { id: number }
  return registered.id
}

export function framed(id: number, value: object): Buffer {
  const prefix = Buffer.alloc(5)
  prefix.writeInt32BE(id, 1)

  return Buffer.concat([prefix, Buffer.from(JSON.stringify(value))])
}

export async function produce(topic: string, partitions: number, count: number, schemaId: number) {
  const producer = kafka.producer()
  await producer.connect()

  try {
    const messages = Array.from({ length: count }, (_, index) => ({
      partition: index % partitions,
      key: `key-${index}`,
      value: framed(schemaId, { id: index, item: `item-${index}` }),
      headers: { source: 'e2e' },
    }))
    await producer.send({ topic, messages })
  } finally {
    await producer.disconnect()
  }
}

export async function seed(name: string, partitions: number, count: number): Promise<number> {
  await createTopic(name, partitions)

  const schemaId = await registerSchema(`${name}-value`, SCHEMA)
  await produce(name, partitions, count, schemaId)
  return schemaId
}

export const rows = (page: Page) => page.getByRole('row').filter({ has: page.getByRole('cell') })

export function detail(page: Page, label: string) {
  return page.locator('dl > div', { has: page.getByRole('term').filter({ hasText: new RegExp(`^${label}$`) }) }).getByRole('definition')
}

export async function indexed(page: Page, records: number) {
  await expect(async () => {
    await page.reload()
    await expect(detail(page, 'Records')).toHaveText(records.toLocaleString('en-US'), { timeout: 2000 })
    await expect(detail(page, 'Snapshot')).toHaveText(/^\d+, /, { timeout: 2000 })
  }).toPass({ timeout: 120_000, intervals: [2000] })
}
