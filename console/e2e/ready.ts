import { ADMIN } from './cluster'

const WAIT_MS = 300_000
const POLL_MS = 1000

export default async function ready() {
  const deadline = Date.now() + WAIT_MS

  while (Date.now() < deadline) {
    const response = await fetch(`${ADMIN}/ready`).catch(() => null)
    if (response?.ok) {
      return
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_MS))
  }

  throw new Error(`${ADMIN} was not ready within ${WAIT_MS / 1000}s`)
}
