import { describe, expect, it } from 'vitest'

import { current, filePage, page, tablePage, withQuery } from '../src/location'

describe('current', () => {
  const sections = ['/', '/tables', '/nodes']
  const tabs = ['/tables/orders', '/tables/orders/partitions', '/tables/orders/files']

  it('picks the longest link the path starts with', () => {
    expect(current('/', sections)).toBe('/')
    expect(current('/tables', sections)).toBe('/tables')
    expect(current('/tables/orders/files/p=0/0-10.parquet', sections)).toBe('/tables')
    expect(current('/tables/orders', tabs)).toBe('/tables/orders')
    expect(current('/tables/orders/files/p=0/0-10.parquet', tabs)).toBe('/tables/orders/files')
  })

  it('matches whole segments only', () => {
    expect(current('/tablesx', sections)).toBe('/')
    expect(current('/tables/orders2', tabs)).toBeUndefined()
  })
})

describe('page', () => {
  it('encodes every segment as is and skips missing ones', () => {
    expect(page('registry', 'a b')).toBe('/registry/a%20b')
    expect(page('nodes', 3, undefined)).toBe('/nodes/3')
  })
})

describe('tablePage and filePage', () => {
  it('link a table, its tabs and its files', () => {
    expect(tablePage('orders', 'partitions')).toBe('/tables/orders/partitions')
    expect(tablePage('orders', undefined)).toBe('/tables/orders')
    expect(filePage('orders', 'p=0/0-10.t0.s0.v1.parquet')).toBe('/tables/orders/files/p%3D0/0-10.t0.s0.v1.parquet')
  })
})

describe('withQuery', () => {
  it('sets, replaces and removes parameters', () => {
    expect(withQuery('/tables', {}, { prefix: 'ord' })).toBe('/tables?prefix=ord')
    expect(withQuery('/tables', { prefix: 'ord' }, { prefix: '' })).toBe('/tables')
    expect(withQuery('/p', { start: 'offset', at: '5' }, { at: 9, record: undefined })).toBe('/p?start=offset&at=9')
  })
})
