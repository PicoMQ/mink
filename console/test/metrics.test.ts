import { describe, expect, it } from 'vitest'

import * as metrics from '../src/api/metrics'

const TEXT = `# TYPE mink_s3_requests_total counter
mink_s3_requests_total{method="get",object="data",status="2xx"} 10
mink_s3_requests_total{method="head",object="data",status="2xx"} 5
mink_indexed_records_total 100
mink_build_duration_seconds_bucket{le="+Inf"} 10
`

describe('parse', () => {
  it('reads names, labels and values and skips comments', () => {
    const scrape = metrics.parse(TEXT, 1000)

    expect(scrape.samples).toHaveLength(4)
    expect(scrape.samples[0]).toEqual({
      name: 'mink_s3_requests_total',
      labels: { method: 'get', object: 'data', status: '2xx' },
      value: 10,
    })
    expect(scrape.samples[3]?.labels.le).toBe('+Inf')
  })

  it('unescapes label values', () => {
    const scrape = metrics.parse('m{name="a\\"b\\\\c\\nd"} 1', 0)

    expect(scrape.samples[0]?.labels.name).toBe('a"b\\c\nd')
  })
})

describe('total', () => {
  it('sums the samples that match every given label', () => {
    const scrape = metrics.parse(TEXT, 0)

    expect(metrics.total(scrape, 'mink_s3_requests_total')).toBe(15)
    expect(metrics.total(scrape, 'mink_s3_requests_total', { method: 'head' })).toBe(5)
    expect(metrics.total(scrape, 'absent')).toBe(0)
  })
})

describe('rate', () => {
  it('divides the counter delta by the elapsed seconds', () => {
    const previous = metrics.parse('mink_indexed_records_total 100', 1000)
    const current = metrics.parse('mink_indexed_records_total 160', 4000)

    expect(metrics.rate(previous, current, 'mink_indexed_records_total')).toBe(20)
  })

  it('has no rate across a counter reset or without elapsed time', () => {
    const before = metrics.parse('mink_indexed_records_total 100', 1000)

    expect(metrics.rate(before, metrics.parse('mink_indexed_records_total 5', 2000), 'mink_indexed_records_total')).toBeNull()
    expect(metrics.rate(before, metrics.parse('mink_indexed_records_total 200', 1000), 'mink_indexed_records_total')).toBeNull()
  })
})

describe('series', () => {
  it('turns consecutive scrapes into per-second rates', () => {
    const at = (atMs: number, value: number) => metrics.parse(`m_total ${value}`, atMs)

    expect(metrics.series([at(0, 0), at(2000, 10), at(4000, 30), at(6000, 5)], 'm_total')).toEqual([5, 10, null])
  })
})

describe('gauge', () => {
  it('reads the summed value of every scrape', () => {
    const at = (value: number) => metrics.parse(`c{l="a"} ${value}\nc{l="b"} 1`, 0)

    expect(metrics.gauge([at(2), at(5)], 'c')).toEqual([3, 6])
    expect(metrics.gauge([at(2), at(5)], 'c', { l: 'a' })).toEqual([2, 5])
  })
})

describe('means', () => {
  it('divides the growth of a histogram sum by the growth of its count', () => {
    const at = (sum: number, count: number) => metrics.parse(`h_sum ${sum}\nh_count ${count}`, 0)

    expect(metrics.means([at(0, 0), at(3, 2), at(3, 2), at(9, 5)], 'h')).toEqual([1.5, null, 2])
  })
})

describe('keep', () => {
  it('appends and drops the oldest past the limit', () => {
    expect(metrics.keep([1, 2], [3], 5)).toEqual([1, 2, 3])
    expect(metrics.keep([1, 2, 3], [4, 5, 6], 4)).toEqual([3, 4, 5, 6])
    expect(metrics.keep([], [1, 2, 3, 4, 5, 6], 2)).toEqual([5, 6])
  })
})
