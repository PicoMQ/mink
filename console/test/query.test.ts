import { afterEach, describe, expect, it, vi } from 'vitest'

import { listing, read } from '../src/api/query'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('listing', () => {
  it('keys by path segments and sends the cursor with the same filters', async () => {
    const urls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) => {
        urls.push(url)
        return new Response(JSON.stringify({ items: [1], next: null }))
      }),
    )

    const pages = listing<number>(['tables', '/a', 'partitions'], { node: 2 })
    const page = await pages.fetch('abc', new AbortController().signal)

    expect(pages.key).toEqual(['tables', '/a', 'partitions', { node: 2 }])
    expect(page).toEqual({ items: [1], next: null })
    expect(urls).toEqual(['/admin/tables/%2Fa/partitions?node=2&cursor=abc'])
  })
})

describe('read', () => {
  it('keys by the path segments and the query', () => {
    expect(read(['tables', 'orders']).queryKey).toEqual(['tables', 'orders', {}])
    expect(read(['tables', 'orders', 'files'], { partition: 1 }).queryKey).toEqual(['tables', 'orders', 'files', { partition: 1 }])
  })
})
