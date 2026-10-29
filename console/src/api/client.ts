type Method = 'GET' | 'POST' | 'PATCH' | 'DELETE'

export type Segments = readonly (string | number)[]

export type Query = Record<string, string | number | boolean | undefined>

interface Options {
  query?: Query
  body?: unknown
  signal?: AbortSignal
}

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

export function admin(...segments: Segments): string {
  return ['/admin', ...segments.map((segment) => encodeURIComponent(segment))].join('/')
}

export async function request<T>(method: Method, path: string, options: Options = {}): Promise<T> {
  const response = await send(method, path, options)

  if (response.status === 202 || response.status === 204) {
    return undefined as T
  }
  return (await response.json()) as T
}

export async function requestText(path: string, options: Options = {}): Promise<string> {
  const response = await send('GET', path, options)

  return response.text()
}

async function send(method: Method, path: string, { query, body, signal }: Options): Promise<Response> {
  const headers = new Headers()
  if (body !== undefined) {
    headers.set('Content-Type', 'application/json')
  }

  const response = await fetch(url(path, query), {
    method,
    headers,
    body: body === undefined ? null : JSON.stringify(body),
    signal: signal ?? null,
  })
  if (response.ok) {
    return response
  }

  throw new ApiError(response.status, await failure(response))
}

function url(path: string, query: Query = {}): string {
  const params = new URLSearchParams()
  for (const [name, value] of Object.entries(query)) {
    if (value !== undefined) {
      params.set(name, String(value))
    }
  }

  const search = params.toString()
  return search ? `${path}?${search}` : path
}

async function failure(response: Response): Promise<string> {
  const body: unknown = await response.json().catch(() => null)

  if (typeof body === 'object' && body !== null && 'error' in body && typeof body.error === 'string') {
    return body.error
  }
  return `${response.status} ${response.statusText}`.trim()
}
