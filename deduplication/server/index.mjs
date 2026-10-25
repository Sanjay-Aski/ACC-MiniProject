import dotenv from 'dotenv'
import express from 'express'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
dotenv.config({ path: resolve(projectRoot, '.env.local') })
dotenv.config({ path: resolve(projectRoot, '.env') })

const app = express()
const port = Number(process.env.PORT || 8787)
const upstream = (process.env.AZURE_FUNCTION_BASE_URL || 'https://func-dedup-lab-10252501.azurewebsites.net').replace(/\/$/, '')
const functionKey = process.env.AZURE_FUNCTION_KEY
const allowedWebOrigins = new Set([
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  ...(process.env.WEB_ORIGINS || '').split(',').map((origin) => origin.trim()).filter(Boolean),
])
const allowed = new Set(['health', 'lab/overview', 'ingest', 'test-status', 'test-events', 'test-record', 'test-output'])

app.use(express.json({ limit: '256kb' }))

app.use('/bff', (request, response, next) => {
  const origin = request.get('origin')
  if (origin && !allowedWebOrigins.has(origin)) {
    return response.status(403).json({ error: 'This website origin is not allowed by the API proxy.' })
  }
  if (origin) {
    response.set('Access-Control-Allow-Origin', origin)
    response.vary('Origin')
  }
  if (request.method === 'OPTIONS') {
    response.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    response.set('Access-Control-Allow-Headers', 'Content-Type')
    response.set('Access-Control-Max-Age', '600')
    return response.sendStatus(204)
  }
  next()
})

app.use('/bff', async (request, response) => {
  const route = request.path.replace(/^\/+/, '')
  if (!allowed.has(route)) return response.status(404).json({ error: 'Unsupported API route' })
  if (!['GET', 'POST'].includes(request.method)) return response.status(405).json({ error: 'Unsupported method' })
  if (route !== 'ingest' && request.method !== 'GET') return response.status(405).json({ error: 'Unsupported method' })

  const target = new URL(`/api/${route}`, upstream)
  Object.entries(request.query).forEach(([key, value]) => {
    if (['testRunId', 'recordId', 'includePayload', 'afterEventId', 'limit'].includes(key)) target.searchParams.set(key, String(value))
  })

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 15000)
  try {
    const upstreamResponse = await fetch(target, {
      method: request.method,
      headers: {
        Accept: 'application/json, text/plain',
        ...(request.method === 'POST' ? { 'Content-Type': 'application/json' } : {}),
        ...(functionKey ? { 'x-functions-key': functionKey } : {}),
      },
      body: request.method === 'POST' ? JSON.stringify(request.body) : undefined,
      signal: controller.signal,
    })
    if (upstreamResponse.status === 401 || upstreamResponse.status === 403) {
      response.status(upstreamResponse.status).json({
        error: 'Azure Function authentication failed. Check AZURE_FUNCTION_KEY in .env.local.',
      })
      return
    }
    response.status(upstreamResponse.status)
    const contentType = upstreamResponse.headers.get('content-type')
    if (contentType) response.set('content-type', contentType)
    if (route === 'test-output' && upstreamResponse.headers.get('content-disposition')) {
      response.set('content-disposition', upstreamResponse.headers.get('content-disposition'))
    }
    response.send(Buffer.from(await upstreamResponse.arrayBuffer()))
  } catch (error) {
    console.error(`Azure Function proxy request failed for ${route}: ${error.name}`)
    response.status(error.name === 'AbortError' ? 504 : 502).json({
      error: error.name === 'AbortError'
        ? 'Azure Function request timed out. Check the Function App status and network access.'
        : 'Azure Function could not be reached through the local proxy. Check AZURE_FUNCTION_BASE_URL and restart the proxy.',
    })
  } finally {
    clearTimeout(timeout)
  }
})

app.listen(port, () => {
  let targetHost = 'invalid URL'
  try { targetHost = new URL(upstream).host } catch { /* report configuration state without printing secrets */ }
  console.log(`BFF listening on http://localhost:${port}`)
  console.log(`Azure Function proxy target: ${targetHost}; Function key: ${functionKey ? 'configured' : 'missing'}`)
})
