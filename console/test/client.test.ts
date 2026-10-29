import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, admin, request } from '../src/api/client'

const calls: { url: string; init: RequestInit }[] = []

function respond(status: number, body: unknown) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init: RequestInit) => {
      calls.push({ url, init })
      return new Response(body === undefined ? null : JSON.stringify(body), { status })
    }),
  )
}

beforeEach(() => {
  calls.length = 0
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('admin', () => {
  it('encodes every segment so a file name with slashes stays one segment', () => {
    expect(admin('tables', 'orders', 'files', 'p=0/0-10.t0.s0.v1.parquet')).toBe(
      '/admin/tables/orders/files/p%3D0%2F0-10.t0.s0.v1.parquet',
    )
  })
})

describe('request', () => {
  it('sends the JSON body and only the defined query values', async () => {
    respond(201, { name: 'orders' })

    const reply = await request<{ name: string }>('POST', '/admin/tables', {
      query: { prefix: 'o', cursor: undefined, limit: 10 },
      body: { topic: 'orders' },
    })

    expect(reply).toEqual({ name: 'orders' })
    expect(calls[0]?.url).toBe('/admin/tables?prefix=o&limit=10')
    expect(new Headers(calls[0]?.init.headers).get('Content-Type')).toBe('application/json')
    expect(calls[0]?.init.body).toBe('{"topic":"orders"}')
  })

  it('returns nothing for an accepted removal', async () => {
    respond(202, undefined)

    expect(await request<void>('DELETE', '/admin/tables/orders')).toBeUndefined()
  })

  it('turns an error body into an error with its message and status', async () => {
    respond(409, { error: 'topic orders is already a table' })

    await expect(request('POST', '/admin/tables')).rejects.toMatchObject({
      status: 409,
      message: 'topic orders is already a table',
    })
    await expect(request('POST', '/admin/tables')).rejects.toBeInstanceOf(ApiError)
  })

  it('falls back to the status when the error body is not the API error format', async () => {
    respond(502, 'upstream')

    await expect(request('GET', '/admin/cluster')).rejects.toMatchObject({ status: 502, message: '502' })
  })
})
