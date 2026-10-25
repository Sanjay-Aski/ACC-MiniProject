import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import './App.css'
import { deleteRecording as deleteSavedRecording, listRecordings, saveRecording } from './recordings.js'

const MAX_BATCH_SIZE = 100
const POLL_INTERVAL_MS = 900
const RETRY_DELAYS_MS = [1000, 2000, 4000, 8000]
const API_PROXY_URL = import.meta.env.VITE_API_PROXY_URL?.trim().replace(/\/+$/, '') || ''
const API_PROXY_BASE = API_PROXY_URL || '/bff'
const STATIC_SITE_WITHOUT_PROXY = import.meta.env.PROD && !API_PROXY_URL

const pipelineNodes = [
  { id: 'client', label: 'Message producer', hint: 'Submitted records', icon: 'send' },
  { id: 'api', label: 'API validation', hint: 'HTTPS · Function auth', icon: 'shield' },
  { id: 'sql_tracking', label: 'SQL run ledger', hint: 'Run + record IDs', icon: 'database' },
  { id: 'service_bus', label: 'Service Bus queue', hint: 'Asynchronous delivery', icon: 'queue' },
  { id: 'function_worker', label: 'Function worker', hint: 'Consumes queued work', icon: 'function' },
  { id: 'fingerprint', label: 'SHA-256 fingerprint', hint: 'Canonical event content', icon: 'hash' },
  { id: 'redis_bloom', label: 'Redis Bloom pre-check', hint: 'Probabilistic hint only', icon: 'redis' },
  { id: 'azure_sql', label: 'Azure SQL decision', hint: 'Authoritative result', icon: 'sql' },
]

const nodeById = Object.fromEntries(pipelineNodes.map((node) => [node.id, node]))
const stageToNode = {
  ingest_received: 'client',
  validated: 'api',
  registered: 'sql_tracking',
  retry_submitted: 'sql_tracking',
  queue_publish: 'service_bus',
  servicebus_queued: 'service_bus',
  worker_received: 'function_worker',
  fingerprint_generated: 'fingerprint',
  bloom_checked: 'redis_bloom',
  sql_decision: 'azure_sql',
  output_route: 'accepted_output',
  processing_failed: 'retry_queue',
  dead_lettered: 'dead_letter',
  ingest_completed: 'service_bus',
}

function Icon({ name, size = 18 }) {
  const paths = {
    lab: <><rect x="4" y="4" width="16" height="16" rx="2" /><path d="M8 8h8M8 12h5M8 16h3" /></>,
    runs: <><path d="M4 6h16M4 12h16M4 18h16" /><circle cx="7" cy="6" r="1" /><circle cx="7" cy="12" r="1" /><circle cx="7" cy="18" r="1" /></>,
    monitor: <><path d="M4 19V5M4 19h16M8 16v-5M12 16V7M16 16v-3" /></>,
    record: <><circle cx="12" cy="12" r="8.5" /><circle cx="12" cy="12" r="3" /></>,
    moon: <path d="M20.5 15.4A8.5 8.5 0 0 1 8.6 3.5 8.5 8.5 0 1 0 20.5 15.4Z" />,
    refresh: <><path d="M20 7v5h-5M4 17v-5h5" /><path d="M5.6 9A7 7 0 0 1 17 6l3 6M4 12l3 6a7 7 0 0 0 11.4-3" /></>,
    send: <><path d="m21 3-7.2 18-3.5-7.3L3 10.2 21 3Z" /><path d="M10.3 13.7 15 9" /></>,
    shield: <><path d="M12 22s8-4 8-11V5l-8-3-8 3v6c0 7 8 11 8 11Z" /><path d="m9 12 2 2 4-4" /></>,
    database: <><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" /></>,
    queue: <><rect x="4" y="4" width="16" height="16" rx="2" /><path d="M8 8h8M8 12h8M8 16h5" /></>,
    function: <><path d="M8 4h8M9 4 7 20M17 4l-2 16M5 10h14M4 15h14" /></>,
    hash: <><path d="M5 9h14M4 15h14M10 4 8 20M16 4l-2 16" /></>,
    redis: <><path d="m12 3 9 4.5-9 4.5-9-4.5L12 3Z" /><path d="m3 12 9 4.5 9-4.5M3 16.5 12 21l9-4.5" /></>,
    sql: <><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" /><path d="m10 10 2 2 3-4" /></>,
    check: <path d="m5 12 4 4L19 6" />,
    alert: <><path d="M12 3 2.8 20h18.4L12 3Z" /><path d="M12 9v4M12 17h.01" /></>,
  }
  return <svg aria-hidden="true" className="icon" height={size} viewBox="0 0 24 24" width={size} fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.7">{paths[name] || paths.lab}</svg>
}

class ApiError extends Error {
  constructor(message, status, body) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.body = body
  }
}

async function api(path, options = {}) {
  if (STATIC_SITE_WITHOUT_PROXY) {
    throw new ApiError('This static site has no backend proxy configured. Set the GitHub Actions variable VITE_API_PROXY_URL to your separately hosted HTTPS BFF endpoint. Keep AZURE_FUNCTION_KEY on the BFF server.', 0)
  }
  let response
  try {
    response = await fetch(`${API_PROXY_BASE}${path}`, {
      ...options,
      headers: { ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers },
    })
  } catch {
    throw new ApiError('Unable to reach the backend proxy. Check VITE_API_PROXY_URL, the BFF WEB_ORIGINS allowlist, and your connection.', 0)
  }
  const contentType = response.headers.get('content-type') || ''
  const body = contentType.includes('json') ? await response.json() : await response.text()
  if (!response.ok) {
    let message = typeof body === 'object' && body?.error ? body.error : `Request failed (${response.status})`
    if (response.status === 413) message = `This request exceeds the ${MAX_BATCH_SIZE}-record batch limit.`
    if (response.status === 409 && path.startsWith('/test-output')) message = 'Output will be available after processing completes.'
    if (response.status === 503 && path === '/ingest') {
      message = 'Queue publishing failed. Retry the remaining batch with the same run ID and line numbers.'
    }
    throw new ApiError(message, response.status, body)
  }
  return body
}

function formatNumber(value, fallback = '—') {
  return typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString() : fallback
}

function formatTime(value) {
  if (!value) return 'Not available'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Not available' : date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function durationBetween(start, end) {
  if (!start || !end) return null
  const elapsed = new Date(end).getTime() - new Date(start).getTime()
  if (!Number.isFinite(elapsed) || elapsed < 0) return null
  return elapsed < 1000 ? `${elapsed} ms` : `${(elapsed / 1000).toFixed(2)} s`
}

function statusLabel(status) {
  const labels = {
    possible_match: 'Possible match · SQL pending',
    no_match: 'No Bloom match · SQL check next',
    unavailable_fallback: 'Redis fallback · SQL continues',
    accepted: 'Accepted by SQL',
    duplicate: 'Duplicate confirmed by SQL',
    accepted_output: 'Accepted output',
    duplicate_sink: 'Duplicate sink',
    retry_pending: 'Retry pending',
    dead_lettered: 'Dead-lettered',
    processing: 'Processing',
    failed: 'Failed',
  }
  return labels[status] || (status ? status.replaceAll('_', ' ') : 'Waiting')
}

function eventNode(event) {
  if (!event) return 'pipeline'
  if (event.stage === 'output_route' && !['accepted_output', 'duplicate_sink'].includes(event.visualNode)) {
    return event.status === 'duplicate' || event.status === 'duplicate_sink' ? 'duplicate_sink' : 'accepted_output'
  }
  const node = event.visualNode || stageToNode[event.stage] || event.stage
  return node === 'dead_letter_or_retry' ? 'retry_queue' : node
}

function eventTone(event) {
  const node = eventNode(event)
  if (['accepted_output'].includes(node) || (node === 'azure_sql' && event?.status === 'accepted')) return 'accepted'
  if (['duplicate_sink'].includes(node) || (node === 'azure_sql' && event?.status === 'duplicate')) return 'duplicate'
  if (['retry_queue', 'dead_letter'].includes(node) || ['failed', 'dead_lettered', 'retry_pending'].includes(event?.status)) return 'failed'
  if (node === 'redis_bloom' && ['possible_match', 'unavailable_fallback'].includes(event?.status)) return 'hint'
  return 'processing'
}

function recordState(event) {
  const node = eventNode(event)
  const completed = ['accepted_output', 'duplicate_sink', 'dead_letter'].includes(node)
  return { currentNode: node, completedStages: [event.stage], status: completed ? event.status : 'processing', lastEvent: event }
}

function parseTextFile(text) {
  const normalized = String(text).replace(/\r\n/g, '\n').replace(/\r/g, '\n')
  const records = normalized.split('\n')
  if (records.length > 1 && records.at(-1) === '') records.pop()
  if (!records.length || (records.length === 1 && records[0] === '')) throw new Error('The selected TXT file is empty.')
  return records
}

function parseJsonRecords(text) {
  const value = JSON.parse(text)
  const records = Array.isArray(value) ? value : Array.isArray(value?.records) ? value.records : [value]
  if (!records.length) throw new Error('The JSON input contains no records.')
  const unsupported = records.findIndex((record) => Array.isArray(record) || typeof record === 'undefined' || typeof record === 'function')
  if (unsupported >= 0) throw new Error(`Record ${unsupported + 1} must be a JSON object or scalar value.`)
  return records
}

function stableStringify(value) {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

function previewFingerprint(value) {
  if (typeof value === 'string') return `txt:${value}`
  if (value === null || typeof value !== 'object') return `scalar:${stableStringify(value)}`
  const normalized = { ...value }
  for (const key of ['event_id', 'eventId', 'event_time', 'eventTime', 'timestamp']) delete normalized[key]
  if (typeof normalized.event_name === 'string') {
    normalized.event_name = normalized.event_name.trim().toLowerCase().replace(/[\s-]+/g, '_')
    if (normalized.event_name === 'payment_failure') normalized.event_name = 'payment_failed'
    if (normalized.event_name === 'payment_successful') normalized.event_name = 'payment_success'
    if (normalized.event_name.includes('order')) {
      return `json:${stableStringify(Object.fromEntries(['event_name', 'user_id', 'order_id'].map((key) => [key, normalized[key] ?? null])))}`
    }
  }
  return `json:${stableStringify(normalized)}`
}

function makeSampleEvents() {
  const records = []
  for (let index = 1; index <= 50; index += 1) {
    const event = {
      event_id: `evt-${String(index).padStart(3, '0')}-a`,
      event_time: new Date(Date.UTC(2026, 9, 25, 10, index)).toISOString(),
      event_name: 'payment_success',
      user_id: `user-${String(index).padStart(3, '0')}`,
      transaction_id: `txn-${String(index).padStart(3, '0')}`,
      amount: index * 10,
    }
    records.push(event, { ...event, event_id: `evt-${String(index).padStart(3, '0')}-b`, event_time: new Date(Date.UTC(2026, 9, 25, 11, index)).toISOString() })
  }
  return records
}

function dependencyTone(dependencies, key) {
  const item = dependencies?.[key]
  if (item?.healthy) return 'healthy'
  if (key === 'redis' && dependencies?.sql?.healthy && dependencies?.serviceBus?.healthy) return 'degraded'
  return 'unavailable'
}

function App() {
  const [page, setPage] = useState('Virtual lab')
  const [health, setHealth] = useState(null)
  const [overview, setOverview] = useState(null)
  const [runId, setRunId] = useState('')
  const [runStatus, setRunStatus] = useState(null)
  const [eventsComplete, setEventsComplete] = useState(false)
  const [events, setEvents] = useState([])
  const [selectedEvent, setSelectedEvent] = useState(null)
  const [selectedRecord, setSelectedRecord] = useState(null)
  const [includePayload, setIncludePayload] = useState(false)
  const [inputMode, setInputMode] = useState('txt')
  const [fileName, setFileName] = useState('')
  const [jsonDraft, setJsonDraft] = useState('')
  const [records, setRecords] = useState([])
  const [recordFormat, setRecordFormat] = useState('TXT')
  const [lineLimit, setLineLimit] = useState('')
  const [error, setError] = useState('')
  const [pollError, setPollError] = useState('')
  const [loading, setLoading] = useState(false)
  const [isDark, setIsDark] = useState(false)
  const [eventFilter, setEventFilter] = useState('all')
  const [animatedEvents, setAnimatedEvents] = useState([])
  const [recordStates, setRecordStates] = useState({})
  const [submission, setSubmission] = useState(null)
  const [runReloadToken, setRunReloadToken] = useState(0)
  const [recordings, setRecordings] = useState([])
  const [replayRecording, setReplayRecording] = useState(null)
  const [replayPlaying, setReplayPlaying] = useState(false)
  const [replaySpeed, setReplaySpeed] = useState(1)
  const cursor = useRef(0)
  const animationQueue = useRef([])
  const queuedEventIds = useRef(new Set())
  const activeRecording = useRef(null)
  const recordingWrites = useRef(Promise.resolve())

  const persistRecording = useCallback((recording) => {
    activeRecording.current = recording
    recordingWrites.current = recordingWrites.current
      .catch(() => undefined)
      .then(() => saveRecording(recording))
      .then(() => listRecordings())
      .then(setRecordings)
      .catch((storageError) => setError(`Could not save the run recording in this browser: ${storageError.message}`))
  }, [])

  useEffect(() => {
    listRecordings().then(setRecordings).catch((storageError) => {
      if (storageError.message.includes('does not support')) setError(storageError.message)
    })
  }, [])

  const refresh = useCallback(async () => {
    const [healthResult, overviewResult] = await Promise.allSettled([api('/health'), api('/lab/overview')])
    if (healthResult.status === 'fulfilled') {
      setHealth(healthResult.value)
    } else {
      const failedHealth = healthResult.reason?.body
      if (failedHealth && typeof failedHealth === 'object' && 'ready' in failedHealth) setHealth(failedHealth)
      else setHealth({ status: 'unreachable', ready: null, dependencies: {}, connectionError: healthResult.reason?.body?.error || healthResult.reason?.message || 'The Function API proxy did not respond.' })
    }
    if (overviewResult.status === 'fulfilled') setOverview(overviewResult.value)
    else if (overviewResult.reason?.body && typeof overviewResult.reason.body === 'object') setOverview(overviewResult.reason.body)
  }, [])

  useEffect(() => {
    refresh()
    const timer = window.setInterval(refresh, 10000)
    return () => window.clearInterval(timer)
  }, [refresh])

  useEffect(() => {
    if (!runId || replayRecording) return undefined
    let cancelled = false
    let timeoutId
    let retries = 0
    const poll = async () => {
      try {
        const statusResult = await api(`/test-status?testRunId=${encodeURIComponent(runId)}`)
        if (cancelled) return
        setRunStatus(statusResult)
        const eventResult = await api(`/test-events?testRunId=${encodeURIComponent(runId)}&afterEventId=${cursor.current}&limit=100`)
        if (cancelled) return
        const incomingEvents = Array.isArray(eventResult.events) ? eventResult.events : []
        const freshEvents = incomingEvents.filter((event) => !queuedEventIds.current.has(event.eventId))
        freshEvents.forEach((event) => {
          animationQueue.current.push(event)
          queuedEventIds.current.add(event.eventId)
        })
        if (freshEvents.length) setEvents((current) => [...current, ...freshEvents].sort((a, b) => a.eventId - b.eventId))
        if (incomingEvents.length) cursor.current = eventResult.nextAfterEventId ?? incomingEvents.at(-1).eventId
        const active = activeRecording.current
        const nextRecordedEvents = active?.testRunId === runId
          ? [...active.events, ...freshEvents].sort((a, b) => a.eventId - b.eventId)
          : null
        if (active?.testRunId === runId && (freshEvents.length || JSON.stringify(active.statusData) !== JSON.stringify(statusResult))) {
          persistRecording({
            ...active,
            status: statusResult.status,
            expectedCount: statusResult.expectedCount,
            createdAt: statusResult.createdAt || active.createdAt,
            updatedAt: new Date().toISOString(),
            statusData: statusResult,
            events: nextRecordedEvents,
          })
        }
        retries = 0
        setPollError('')
        if (statusResult.status === 'completed' && !eventResult.hasMore) {
          setEventsComplete(true)
          return
        }
        timeoutId = window.setTimeout(poll, POLL_INTERVAL_MS)
      } catch (requestError) {
        if (cancelled) return
        if (retries >= RETRY_DELAYS_MS.length) {
          setPollError(`${requestError.message} Polling paused after repeated failures.`)
          return
        }
        setPollError(requestError.message)
        const wait = RETRY_DELAYS_MS[Math.min(retries, RETRY_DELAYS_MS.length - 1)]
        retries += 1
        timeoutId = window.setTimeout(poll, wait)
      }
    }
    poll()
    return () => {
      cancelled = true
      window.clearTimeout(timeoutId)
    }
  }, [runId, runReloadToken, replayRecording, persistRecording])

  useEffect(() => {
    if (replayRecording && (!replayPlaying || page !== 'Virtual lab')) return undefined
    const timer = window.setInterval(() => {
      const batch = []
      const seenRecords = new Set()
      for (let index = 0; index < animationQueue.current.length && batch.length < 20;) {
        const event = animationQueue.current[index]
        const recordKey = event.recordId || `run-event-${event.eventId}`
        if (seenRecords.has(recordKey)) {
          index += 1
          continue
        }
        seenRecords.add(recordKey)
        batch.push(animationQueue.current.splice(index, 1)[0])
      }
      if (!batch.length) {
        if (replayRecording) setReplayPlaying(false)
        return
      }
      setAnimatedEvents((current) => [...current, ...batch])
      setRecordStates((current) => batch.reduce((states, event) => {
        if (!event.recordId) return states
        const previous = states[event.recordId]
        const next = recordState(event)
        return {
          ...states,
          [event.recordId]: {
            ...next,
            completedStages: [...new Set([...(previous?.completedStages || []), ...next.completedStages])],
          },
        }
      }, current))
    }, replayRecording ? Math.max(70, 350 / replaySpeed) : 350)
    return () => window.clearInterval(timer)
  }, [replayRecording, replayPlaying, replaySpeed, page])

  useEffect(() => {
    if (!selectedEvent?.recordId || !selectedEvent.testRunId) {
      setSelectedRecord(null)
      return undefined
    }
    let cancelled = false
    const include = includePayload ? '&includePayload=true' : ''
    api(`/test-record?testRunId=${encodeURIComponent(selectedEvent.testRunId)}&recordId=${encodeURIComponent(selectedEvent.recordId)}${include}`)
      .then((response) => { if (!cancelled) setSelectedRecord(response.record) })
      .catch(() => { if (!cancelled) setSelectedRecord(null) })
    return () => { cancelled = true }
  }, [selectedEvent, includePayload])

  const requestedCount = lineLimit === '' ? records.length : Number(lineLimit)
  const inputCount = Number.isInteger(requestedCount) ? Math.max(0, Math.min(records.length, requestedCount)) : 0
  const selectedRecords = records.slice(0, inputCount)
  const duplicateEstimate = useMemo(() => {
    const seen = new Set()
    let matches = 0
    selectedRecords.forEach((record) => {
      const key = previewFingerprint(record)
      if (seen.has(key)) matches += 1
      else seen.add(key)
    })
    return { unique: seen.size, matches }
  }, [selectedRecords])

  const loadRecords = (newRecords, name, format) => {
    setRecords(newRecords)
    setFileName(name)
    setRecordFormat(format)
    setLineLimit(String(newRecords.length))
    setError('')
  }

  const loadFile = (file) => {
    if (!file) return
    const extension = file.name.toLowerCase().split('.').at(-1)
    if (!['txt', 'json'].includes(extension)) {
      setError('Choose a UTF-8 TXT or JSON file.')
      return
    }
    setRecords([])
    setFileName('')
    setLineLimit('')
    const reader = new FileReader()
    reader.onload = () => {
      try {
        const text = String(reader.result || '')
        if (extension === 'txt') {
          setInputMode('txt')
          loadRecords(parseTextFile(text), file.name, 'TXT')
        } else {
          const parsed = parseJsonRecords(text)
          setInputMode('json')
          setJsonDraft(text)
          loadRecords(parsed, file.name, 'JSON')
        }
      } catch (parseError) {
        setError(parseError.message || 'The selected file could not be parsed.')
      }
    }
    reader.onerror = () => setError('The selected file could not be read.')
    reader.readAsText(file)
  }

  const parseJsonDraft = () => {
    try {
      const parsed = parseJsonRecords(jsonDraft)
      loadRecords(parsed, 'Pasted JSON', 'JSON')
    } catch (parseError) {
      setError(parseError instanceof SyntaxError ? `Invalid JSON: ${parseError.message}` : parseError.message)
    }
  }

  const submitBatches = async (activeSubmission) => {
    setLoading(true)
    setError('')
    let nextOffset = activeSubmission.nextOffset
    try {
      while (nextOffset < activeSubmission.records.length) {
        const batch = activeSubmission.records.slice(nextOffset, nextOffset + MAX_BATCH_SIZE)
        const response = await api('/ingest', {
          method: 'POST',
          body: JSON.stringify({
            testRunId: activeSubmission.testRunId,
            expectedCount: activeSubmission.expectedCount,
            lineNumberStart: nextOffset + 1,
            records: batch,
          }),
        })
        const count = Number(response.recordsSubmitted || batch.length)
        nextOffset += count
        const updated = { ...activeSubmission, nextOffset }
        setSubmission(updated)
        if (runId !== activeSubmission.testRunId) setRunId(activeSubmission.testRunId)
      }
      setSubmission({ ...activeSubmission, nextOffset, allSubmitted: true })
      setRunId(activeSubmission.testRunId)
      setError('')
    } catch (requestError) {
      const updated = { ...activeSubmission, nextOffset }
      setSubmission(updated)
      setRunId(activeSubmission.testRunId)
      setError(requestError.message)
    } finally {
      setLoading(false)
    }
  }

  const startRun = async () => {
    if (!selectedRecords.length) return setError('Load a non-empty TXT or JSON input first.')
    const count = Number(lineLimit)
    if (!Number.isInteger(count) || count < 1 || count > records.length) return setError(`Choose between 1 and ${records.length} records.`)
    if (health?.ready !== true) return setError(health?.ready === false ? 'SQL or Service Bus is unavailable. Wait for the health check to recover.' : health?.connectionError || 'Waiting for the backend health check before starting a run.')
    const newRunId = crypto.randomUUID()
    const nextSubmission = { testRunId: newRunId, expectedCount: count, records: records.slice(0, count), nextOffset: 0, allSubmitted: false }
    const recordingTime = new Date().toISOString()
    persistRecording({
      testRunId: newRunId,
      expectedCount: count,
      status: 'submitting',
      createdAt: recordingTime,
      updatedAt: recordingTime,
      statusData: { status: 'submitting', expectedCount: count, submittedCount: 0, acceptedCount: 0, duplicateCount: 0, failedCount: 0, progressPercent: 0 },
      events: [],
    })
    setReplayRecording(null)
    setReplayPlaying(false)
    setSubmission(nextSubmission)
    setRunId(newRunId)
    setRunStatus(null)
    setEventsComplete(false)
    setPollError('')
    setEvents([])
    setAnimatedEvents([])
    setRecordStates({})
    animationQueue.current = []
    queuedEventIds.current.clear()
    cursor.current = 0
    setSelectedEvent(null)
    setIncludePayload(false)
    await submitBatches(nextSubmission)
  }

  const retryRemainingBatches = async () => {
    if (!submission || submission.nextOffset >= submission.records.length) return
    if (health?.ready !== true) return setError(health?.ready === false ? 'SQL or Service Bus is unavailable. Wait for the health check to recover.' : health?.connectionError || 'Waiting for the backend health check before retrying this run.')
    await submitBatches(submission)
  }

  const downloadOutput = async () => {
    if (!runId || runStatus?.status !== 'completed') return
    try {
      const output = await api(`/test-output?testRunId=${encodeURIComponent(runId)}`)
      const url = URL.createObjectURL(new Blob([output], { type: 'text/plain;charset=utf-8' }))
      const link = document.createElement('a')
      link.href = url
      link.download = 'accepted-output.txt'
      link.click()
      window.setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (downloadError) {
      setError(downloadError.message)
    }
  }

  const startReplay = (recording) => {
    const savedEvents = [...(recording.events || [])].sort((a, b) => a.eventId - b.eventId)
    setReplayRecording(recording)
    setReplayPlaying(true)
    setReplaySpeed(1)
    setPage('Virtual lab')
    setRunId(recording.testRunId)
    setRunStatus(recording.statusData || { status: recording.status, expectedCount: recording.expectedCount })
    setEventsComplete(true)
    setEvents(savedEvents)
    setAnimatedEvents([])
    setRecordStates({})
    animationQueue.current = savedEvents.slice()
    queuedEventIds.current = new Set(savedEvents.map((event) => event.eventId))
    cursor.current = savedEvents.at(-1)?.eventId || 0
    setSelectedEvent(null)
    setSelectedRecord(null)
    setIncludePayload(false)
    setPollError('')
    setEventFilter('all')
  }

  const restartReplay = () => {
    if (!replayRecording) return
    const savedEvents = [...(replayRecording.events || [])].sort((a, b) => a.eventId - b.eventId)
    setEvents(savedEvents)
    setAnimatedEvents([])
    setRecordStates({})
    animationQueue.current = savedEvents.slice()
    queuedEventIds.current = new Set(savedEvents.map((event) => event.eventId))
    setSelectedEvent(null)
    setReplayPlaying(true)
  }

  const seekReplay = (position) => {
    if (!replayRecording) return
    const savedEvents = [...(replayRecording.events || [])].sort((a, b) => a.eventId - b.eventId)
    const index = Math.max(0, Math.min(savedEvents.length, Number(position) || 0))
    const playedEvents = savedEvents.slice(0, index)
    const states = playedEvents.reduce((current, event) => {
      if (!event.recordId) return current
      const previous = current[event.recordId]
      const next = recordState(event)
      return {
        ...current,
        [event.recordId]: {
          ...next,
          completedStages: [...new Set([...(previous?.completedStages || []), ...next.completedStages])],
        },
      }
    }, {})
    setEvents(savedEvents)
    setAnimatedEvents(playedEvents)
    setRecordStates(states)
    animationQueue.current = savedEvents.slice(index)
    setSelectedEvent(null)
    setReplayPlaying(index < savedEvents.length)
  }

  const exitReplay = () => {
    setReplayRecording(null)
    setReplayPlaying(false)
    setRunId('')
    setRunStatus(null)
    setEventsComplete(false)
    setEvents([])
    setAnimatedEvents([])
    setRecordStates({})
    animationQueue.current = []
    queuedEventIds.current.clear()
    cursor.current = 0
    setSelectedEvent(null)
    setSelectedRecord(null)
  }

  const removeRecording = async (testRunId) => {
    try {
      await deleteSavedRecording(testRunId)
      setRecordings(await listRecordings())
      if (replayRecording?.testRunId === testRunId) exitReplay()
    } catch (storageError) {
      setError(`Could not delete the local recording: ${storageError.message}`)
    }
  }

  const visibleEvents = replayRecording ? animatedEvents : animatedEvents.length ? animatedEvents : events
  const timelineEvents = replayRecording ? animatedEvents : events
  const latestByStage = Object.fromEntries(visibleEvents.map((event) => [eventNode(event), event]))
  const nodeCounts = visibleEvents.reduce((counts, event) => {
    const node = eventNode(event)
    counts[node] = (counts[node] || 0) + 1
    return counts
  }, {})
  const selectedStage = selectedEvent ? eventNode(selectedEvent) : ''
  const eventRows = useMemo(() => timelineEvents.filter((event) => {
    if (eventFilter === 'all') return true
    if (eventFilter === 'processing') return !['accepted', 'duplicate', 'failed', 'accepted_output', 'duplicate_sink', 'dead_lettered'].includes(event.status)
    if (eventFilter === 'unique') return event.status === 'accepted' || eventNode(event) === 'accepted_output'
    if (eventFilter === 'duplicate') return event.status === 'duplicate' || eventNode(event) === 'duplicate_sink'
    return ['failed', 'retry_pending', 'dead_lettered'].includes(event.status) || ['retry_queue', 'dead_letter'].includes(eventNode(event))
  }), [timelineEvents, eventFilter])
  const runMetrics = runStatus || {}
  const overviewMetrics = overview?.lastHour || {}
  const healthTone = health?.ready === null ? 'unavailable' : health?.status || 'checking'
  const healthLabel = health?.ready === null ? 'unreachable' : health?.status || 'checking'
  const dependencies = health?.dependencies || overview || {}
  const retryable = submission && submission.nextOffset < submission.records.length
  const runCanStart = !runId || (runStatus?.status === 'completed' && eventsComplete && animatedEvents.length >= events.length)
  const replayComplete = Boolean(replayRecording && !replayPlaying && animatedEvents.length >= events.length)

  return (
    <div className={isDark ? 'lab-shell dark' : 'lab-shell'}>
      <aside className="lab-sidebar">
        <div className="lab-brand"><div className="azure-mark">A</div><div><strong>Azure</strong><span>Deduplication Virtual Lab</span></div></div>
        <div className="environment"><i /> Development environment</div>
        <nav aria-label="Main navigation">
          {[
            ['Virtual lab', 'lab'],
            ['Runs', 'runs'],
            ['Recordings', 'record'],
            ['Monitoring', 'monitor'],
          ].map(([item, icon]) => <button aria-current={page === item ? 'page' : undefined} className={page === item ? 'side-link active' : 'side-link'} key={item} onClick={() => setPage(item)} type="button"><Icon name={icon} />{item}</button>)}
        </nav>
        <div className="side-footer"><button className="side-link" onClick={() => setIsDark(!isDark)} type="button"><Icon name="moon" />{isDark ? 'Light theme' : 'Dark theme'}</button><span className="version">SQL authoritative · v1.1</span></div>
      </aside>

      <main className="lab-main">
        <header className="lab-header">
          <div><span className="crumb">VIRTUAL LAB <span>/</span> {page.toUpperCase()}</span><h1>Azure Deduplication Virtual Lab</h1></div>
          <div className="header-tools"><span className={`connection ${healthTone}`}><i />{healthLabel}</span><span className="updated">Updated {formatTime(health?.timestamp)}</span><button aria-label="Refresh health and overview" className="refresh-button" onClick={refresh} type="button"><Icon name="refresh" /></button></div>
        </header>
        {health?.ready && health.status === 'degraded' && <div className="health-banner degraded"><Icon name="alert" /><span><strong>Redis is unavailable.</strong> Processing can continue; Azure SQL remains the final deduplication authority.</span></div>}
        {health?.ready === false && <div className="health-banner unavailable"><Icon name="alert" /><span><strong>Pipeline unavailable.</strong> SQL or Service Bus is not ready, so new runs are paused.</span></div>}
        {health?.ready === null && health.connectionError && <div className="health-banner unavailable"><Icon name="alert" /><span><strong>Backend connection failed.</strong> {health.connectionError}{!STATIC_SITE_WITHOUT_PROXY && <> For local development, start the proxy and check its server-side Azure settings.</>}</span></div>}
        {(error || pollError) && <div className="error-banner" role="alert"><Icon name="alert" /><span>{error || pollError}</span>{pollError && <button className="retry-poll" onClick={() => { setPollError(''); setRunReloadToken((value) => value + 1) }} type="button">Retry polling</button>}<button aria-label="Dismiss message" onClick={() => { setError(''); setPollError('') }} type="button">×</button></div>}

        {page === 'Virtual lab' && replayRecording && <ReplayControls recording={replayRecording} current={animatedEvents.length} total={events.length} playing={replayPlaying} speed={replaySpeed} onToggle={() => { if (replayComplete) restartReplay(); else setReplayPlaying((value) => !value) }} onRestart={restartReplay} onSeek={seekReplay} onSpeedChange={(value) => setReplaySpeed(Number(value))} onExit={exitReplay} />}

        {page === 'Virtual lab' && <div className="lab-grid">
          <section className="metrics-strip" aria-label="Run metrics">
            {[
              ['Submitted', runMetrics.submittedCount ?? overviewMetrics.submittedCount, 'records received'],
              ['Queued', runMetrics.queuedCount ?? overviewMetrics.queued, 'Service Bus confirmed'],
              ['Accepted', runMetrics.acceptedCount ?? overviewMetrics.accepted, 'unique records'],
              ['Duplicates', runMetrics.duplicateCount ?? overviewMetrics.duplicates, 'confirmed by SQL'],
              ['Failed', runMetrics.failedCount ?? overviewMetrics.failed, 'failed or dead-lettered'],
            ].map(([label, value, detail]) => <article className="metric" key={label}><span>{label}</span><strong>{formatNumber(value)}</strong><small>{detail}</small></article>)}
          </section>

          {!replayRecording && <section className="control-panel panel">
            <div className="panel-title"><div><span className="kicker">INGEST A TEST RUN</span><h2>Submit records to the live pipeline</h2><p className="panel-subtitle">A 202 response means accepted for queueing. The run finishes after the worker events arrive.</p></div><span className="live-badge"><i />LIVE STATUS</span></div>
            <div className="input-modes" role="tablist" aria-label="Input format">
              <button aria-selected={inputMode === 'txt'} className={inputMode === 'txt' ? 'mode-button active' : 'mode-button'} onClick={() => setInputMode('txt')} role="tab" type="button">TXT file</button>
              <button aria-selected={inputMode === 'json'} className={inputMode === 'json' ? 'mode-button active' : 'mode-button'} onClick={() => setInputMode('json')} role="tab" type="button">JSON events</button>
            </div>
            <div className="input-row">
              <label className="file-picker"><input accept=".txt,.json,text/plain,application/json" onChange={(event) => loadFile(event.target.files?.[0])} type="file" /><span className="upload-icon">＋</span><span className="file-label">{fileName || (inputMode === 'txt' ? 'Choose UTF-8 .txt file' : 'Choose .json file')}</span><small>Browse</small></label>
              <label className="line-input">Records <input aria-label="Number of records to submit" max={records.length || undefined} min="1" onChange={(event) => setLineLimit(event.target.value)} type="number" value={lineLimit} /></label>
              <button className="run-button" disabled={loading || !selectedRecords.length || health?.ready !== true || retryable || !runCanStart} onClick={startRun} type="button">{loading ? 'Submitting…' : runCanStart ? 'Start test run' : 'Run in progress'} <span>→</span></button>
              {retryable && <button className="secondary-button" disabled={loading || health?.ready !== true} onClick={retryRemainingBatches} type="button">Retry remaining batches</button>}
            </div>
            {inputMode === 'json' && <div className="json-entry"><label htmlFor="json-events">Paste a JSON array or an object with a records array</label><textarea id="json-events" onChange={(event) => { setJsonDraft(event.target.value); setRecords([]); setFileName(''); setLineLimit('') }} placeholder={'[{"event_name":"payment_success","user_id":"u-10"}]'} value={jsonDraft} /><div className="json-actions"><span>Payload is sent through the server-side API proxy.</span><button className="secondary-button" onClick={parseJsonDraft} type="button">Parse pasted JSON</button><button className="sample-button" onClick={() => { const sample = makeSampleEvents(); setJsonDraft(JSON.stringify(sample, null, 2)); loadRecords(sample, '100 sample events', 'JSON'); setInputMode('json') }} type="button">Generate 100 sample events</button></div></div>}
            <div className="preview-panel">
              {selectedRecords.length ? <>
                <div className="preview-summary"><div><span className="preview-number">{formatNumber(inputCount)}</span><span>records loaded</span></div><div><span className="preview-number">{recordFormat}</span><span>input format</span></div><div><span className="preview-number">{formatNumber(duplicateEstimate.unique)}</span><span>unique within file</span></div><div><span className="preview-number">{formatNumber(duplicateEstimate.matches)}</span><span>duplicate candidates</span></div><div><span className="preview-number">{formatNumber(Math.ceil(inputCount / MAX_BATCH_SIZE))}</span><span>API batches</span></div></div>
                <div className="preview-records"><span>Preview · whitespace in TXT records is preserved</span><div>{selectedRecords.slice(0, 3).map((record, index) => <code key={`record-${index}`}>{index + 1}: {typeof record === 'string' ? record || '(empty line)' : JSON.stringify(record)}</code>)}</div></div>
                {duplicateEstimate.matches > 0 && <p className="estimate-note">The estimate compares records within this file. Earlier records in the one-hour SQL window can change the final accepted and duplicate counts.</p>}
              </> : <div className="empty-preview"><span className="empty-file">＋</span><div><strong>Choose a file or load JSON events</strong><span>TXT lines are kept exactly as written. JSON objects are normalized by the backend before fingerprinting.</span></div></div>}
            </div>
            {runId && <div className="run-meta"><div><span className="meta-label">RUN ID</span><code>{runId}</code></div><div className="run-progress"><span className={runStatus?.status === 'completed' ? 'complete' : ''}>{runStatus?.status || 'processing'} · {runStatus?.progressPercent ?? 0}%</span><div className="progress-track"><i style={{ width: `${Math.max(0, Math.min(100, Number(runStatus?.progressPercent) || 0))}%` }} /></div></div><span className="batch-meta">{submission ? `${submission.nextOffset} / ${submission.expectedCount} submitted` : ''}</span></div>}
          </section>}

          {runStatus?.status === 'completed' && (!replayRecording || replayComplete) && <section className="completion-panel panel"><div className="completion-heading"><span className="completion-icon"><Icon name="check" /></span><div><span className="kicker">RUN COMPLETE</span><h2>Processing finished</h2><p>{replayRecording ? 'Replay finished. This is the saved final summary for the original run.' : 'Final decisions are recorded by Azure SQL. Download includes accepted output only.'}</p></div></div><div className="completion-stats"><div><span>Submitted</span><strong>{formatNumber(runStatus.submittedCount)}</strong></div><div><span>Accepted</span><strong className="accepted-text">{formatNumber(runStatus.acceptedCount)}</strong></div><div><span>Duplicates</span><strong className="duplicate-text">{formatNumber(runStatus.duplicateCount)}</strong></div><div><span>Failed</span><strong className="failed-text">{formatNumber(runStatus.failedCount)}</strong></div><div><span>Processing time</span><strong>{durationBetween(runStatus.createdAt, events.at(-1)?.occurredAt) || '—'}</strong></div></div><div className="completion-footer"><span>SQL {dependencies.sql?.healthy ? 'healthy' : 'unavailable'} · Service Bus {dependencies.serviceBus?.healthy ? 'healthy' : 'unavailable'} · Redis {dependencies.redis?.healthy ? 'healthy' : 'fallback active'}</span>{!replayRecording && <button className="download-button" onClick={downloadOutput} type="button">Download accepted output ↓</button>}</div></section>}

          <section className="pipeline panel">
            <div className="panel-title pipeline-heading"><div><span className="kicker">OBSERVED MESSAGE PATH</span><h2>Deduplication pipeline</h2><p className="panel-subtitle">Tokens move only when persisted backend events arrive. Dashed groups show platform context.</p></div><div className="legend"><span><i className="dot processing" /> In transit</span><span><i className="dot accepted" /> Accepted by SQL</span><span><i className="dot duplicate" /> Duplicate by SQL</span></div></div>
            <PipelineVisualization recordStates={recordStates} nodeCounts={nodeCounts} latestByStage={latestByStage} selectedStage={selectedStage} setSelectedEvent={setSelectedEvent} redisHealthy={health?.dependencies?.redis?.healthy} />
            <p className="pipeline-note"><strong>SQL makes the final decision.</strong> A Bloom possible match is only a hint; an unavailable Redis path falls back to SQL. Stages without persisted events remain visually idle.</p>
          </section>

          <div className="lower-grid">
            <section className="timeline panel"><div className="panel-title"><div><span className="kicker">{replayRecording ? 'RECORDED EVENTS' : 'PERSISTED EVENTS'}</span><h2>Event timeline <span className="count">{timelineEvents.length}</span></h2><p className="panel-subtitle">{animatedEvents.length} of {events.length} events {replayRecording ? 'replayed' : 'animated'}</p></div><select aria-label="Filter events" onChange={(event) => setEventFilter(event.target.value)} value={eventFilter}><option value="all">All events</option><option value="processing">Processing</option><option value="unique">Unique</option><option value="duplicate">Duplicate</option><option value="failed">Failed / retry</option></select></div><div className="event-list">{eventRows.length ? eventRows.slice().reverse().map((event) => <button className={selectedEvent?.eventId === event.eventId ? 'event-row selected' : 'event-row'} key={event.eventId} onClick={() => { setSelectedEvent(event); setIncludePayload(false) }} type="button"><span className="event-time">{formatTime(event.occurredAt)}</span><span className="event-stage">{nodeById[eventNode(event)]?.label || eventNode(event).replaceAll('_', ' ')}</span><span className="event-record">{event.recordId ? `${event.recordId.slice(0, 8)}…` : 'run'}</span><b className={`event-status ${eventTone(event)}`}>{statusLabel(event.status)}</b></button>) : <div className="empty-state">{replayRecording ? 'Press play to replay the recorded event stream.' : 'No persisted events yet. Start a run to watch real records move through the pipeline.'}</div>}</div></section>
            <Inspector event={selectedEvent} record={selectedRecord} runId={runId} onShowPayload={() => setIncludePayload(true)} />
          </div>
        </div>}

        {page === 'Runs' && <RunsPage overview={overview} onInspect={(run) => {
          const saved = recordings.find((item) => item.testRunId === run.testRunId)
          if (saved?.events?.length) {
            startReplay(saved)
            return
          }
          const recording = saved || { testRunId: run.testRunId, expectedCount: run.expectedCount, status: run.status, createdAt: run.createdAt || new Date().toISOString(), updatedAt: new Date().toISOString(), statusData: run, events: [] }
          persistRecording(recording)
          setReplayRecording(null)
          setReplayPlaying(false)
          setRunId(run.testRunId)
          setRunReloadToken((value) => value + 1)
          setRunStatus(recording.statusData || run)
          setEventsComplete(false)
          setEvents([])
          setAnimatedEvents([])
          setRecordStates({})
          animationQueue.current = []
          queuedEventIds.current.clear()
          cursor.current = 0
          setSelectedEvent(null)
          setPage('Virtual lab')
        }} />}
        {page === 'Recordings' && <RecordingsPage recordings={recordings} onReplay={startReplay} onDelete={removeRecording} />}
        {page === 'Monitoring' && <Monitoring health={health} overview={overview} onRefresh={refresh} />}
      </main>
    </div>
  )
}

function PipelineNode({ id, label, hint, icon, events = [], count = 0, selected = false, onSelect, compact = false, tone = '' }) {
  const latest = events.at(-1)
  const currentTokens = events.filter((event) => event.recordId).slice(-4)
  const status = latest ? statusLabel(latest.status) : hint
  const className = ['architecture-node', selected ? 'selected' : '', latest ? `has-event tone-${eventTone(latest)}` : '', compact ? 'compact' : '', tone].filter(Boolean).join(' ')
  return <div className="architecture-node-wrap">
    <div aria-label={`${label}. ${count} observed events. ${status}`} className={className} role="group">
      <button className="node-select" disabled={!latest} onClick={() => latest && onSelect(latest)} type="button">
        <span className="node-topline"><span className={`node-icon ${icon}`}><Icon name={icon} size={16} /></span><span className="node-count">{formatNumber(count, '0')}</span></span>
        <strong>{label}</strong><small>{status}</small>
      </button>
      {currentTokens.length > 0 && <div aria-label="Records currently at this stage" className="token-stack">{currentTokens.map((event) => <button aria-label={`Inspect record ${event.recordId}`} className={`event-token ${eventTone(event)}`} key={`${id}-${event.recordId}`} onClick={() => onSelect(event)} title={`Inspect record ${event.recordId}`} type="button">{event.recordId.slice(0, 5)}</button>)}</div>}
    </div>
  </div>
}

function PipelineVisualization({ recordStates, nodeCounts, latestByStage, selectedStage, setSelectedEvent, redisHealthy }) {
  const eventsAt = (id) => Object.values(recordStates).filter((state) => state.currentNode === id).map((state) => state.lastEvent).sort((a, b) => a.eventId - b.eventId)
  const node = (id) => nodeById[id]
  return <div className="pipeline-scroll" tabIndex="0" aria-label="Horizontally scrollable pipeline diagram">
    <div className="architecture-canvas">
      <div className="architecture-security"><div className="security-title"><span className="security-mark"><Icon name="shield" size={16} /></span><div><strong>Security &amp; access control</strong><small>Request and service safeguards</small></div></div><div className="security-items"><span>TLS in transit</span><span>Function key via server proxy</span><span>Input validation</span><span>Managed identity for Redis / Service Bus</span></div></div>
      <div className="architecture-observability"><span className="observe-icon"><Icon name="monitor" size={17} /></span><div><strong>Azure Monitor</strong><small>Platform diagnostics · outside run event feed</small></div></div>

      <div className="architecture-main">
        <div className="architecture-group producer-group"><span className="group-label">SOURCE</span><PipelineNode id="client" {...node('client')} count={nodeCounts.client} events={eventsAt('client')} selected={selectedStage === 'client'} onSelect={setSelectedEvent} /></div>
        <div className="architecture-arrow source-arrow" aria-hidden="true"><span /></div>
        <div className="architecture-group ingestion-group"><span className="group-label">MESSAGE INGESTION</span><PipelineNode id="api" {...node('api')} count={nodeCounts.api} events={eventsAt('api')} selected={selectedStage === 'api'} onSelect={setSelectedEvent} compact /><div className="mini-flow-arrow" aria-hidden="true">↓</div><PipelineNode id="sql_tracking" {...node('sql_tracking')} count={nodeCounts.sql_tracking} events={eventsAt('sql_tracking')} selected={selectedStage === 'sql_tracking'} onSelect={setSelectedEvent} compact /><div className="mini-flow-arrow" aria-hidden="true">↓</div><PipelineNode id="service_bus" {...node('service_bus')} count={nodeCounts.service_bus} events={eventsAt('service_bus')} selected={selectedStage === 'service_bus'} onSelect={setSelectedEvent} compact /><div className="dead-letter-mini"><span>Dead Letter Queue</span><PipelineNode id="dead_letter" label="Dead-lettered" hint="Timer reconciliation" icon="queue" count={nodeCounts.dead_letter} events={eventsAt('dead_letter')} selected={selectedStage === 'dead_letter'} onSelect={setSelectedEvent} compact /></div></div>
        <div className="architecture-arrow middleware-arrow" aria-hidden="true"><span /></div>
        <div className="middleware-boundary"><div className="middleware-heading"><span className="group-label">MIDDLEWARE LAYER</span><span>Persisted stages animate below</span></div><div className="middleware-flow">
          <PipelineNode id="function_worker" {...node('function_worker')} count={nodeCounts.function_worker} events={eventsAt('function_worker')} selected={selectedStage === 'function_worker'} onSelect={setSelectedEvent} compact />
          <div className="inner-arrow" aria-hidden="true"><span /></div>
          <PipelineNode id="fingerprint" {...node('fingerprint')} count={nodeCounts.fingerprint} events={eventsAt('fingerprint')} selected={selectedStage === 'fingerprint'} onSelect={setSelectedEvent} compact />
          <div className="inner-arrow" aria-hidden="true"><span /></div>
          <PipelineNode id="redis_bloom" {...node('redis_bloom')} count={nodeCounts.redis_bloom} events={eventsAt('redis_bloom')} selected={selectedStage === 'redis_bloom'} onSelect={setSelectedEvent} compact />
          <div className="inner-arrow" aria-hidden="true"><span /></div>
          <PipelineNode id="azure_sql" {...node('azure_sql')} count={nodeCounts.azure_sql} events={eventsAt('azure_sql')} selected={selectedStage === 'azure_sql'} onSelect={setSelectedEvent} compact tone="authoritative" />
        </div><div className="decision-caption"><span className="decision-diamond" />SQL checks the one-hour fingerprint window and records the decision</div>
          <div className="middleware-branches"><div className="branch-rail" aria-hidden="true"><span /><span /><span /></div><PipelineNode id="retry_queue" label="Retry / reconciliation" hint="Transient failure path" icon="queue" count={nodeCounts.retry_queue} events={eventsAt('retry_queue')} selected={selectedStage === 'retry_queue'} onSelect={setSelectedEvent} compact /><PipelineNode id="accepted_output" label="Accepted output" hint="SQL accepted" icon="check" count={nodeCounts.accepted_output} events={eventsAt('accepted_output')} selected={selectedStage === 'accepted_output'} onSelect={setSelectedEvent} compact /><PipelineNode id="duplicate_sink" label="Duplicate sink" hint="SQL duplicate" icon="database" count={nodeCounts.duplicate_sink} events={eventsAt('duplicate_sink')} selected={selectedStage === 'duplicate_sink'} onSelect={setSelectedEvent} compact /></div>
        </div>
        <div className="architecture-arrow output-arrow" aria-hidden="true"><span /></div>
        <div className="architecture-group consumers-group"><span className="group-label">OUTPUTS</span><div className="consumer-card"><span className="consumer-dot accepted" /><div><strong>Accepted file</strong><small>Download when complete</small></div></div><div className="consumer-card"><span className="consumer-dot sql" /><div><strong>SQL run state</strong><small>Records + event timeline</small></div></div><div className="consumer-card reference-consumer"><span className="consumer-dot muted" /><div><strong>Other consumers</strong><small>Reference integration point</small></div></div></div>
      </div>

      <div className="redis-state-row"><div className="redis-state-connector" aria-hidden="true" /><div className="redis-state-card"><span className="redis-state-icon"><Icon name="redis" size={18} /></span><div className="redis-state-copy"><strong>Azure Managed Redis</strong><small>Bloom pre-check state · 5-minute buckets · 1-hour window</small></div><span className={`redis-health ${redisHealthy === false ? 'degraded' : latestByStage.redis_bloom ? healthClass(latestByStage.redis_bloom) : redisHealthy ? 'observed' : 'idle'}`}>{redisHealthy === false ? 'Fallback active' : latestByStage.redis_bloom ? statusLabel(latestByStage.redis_bloom.status) : redisHealthy ? 'Healthy · hint only' : 'Pre-check hint'}</span></div><p>Redis can suggest a possible match; it never rejects a record. SQL remains authoritative if Redis is unavailable.</p></div>
      <div className="canvas-footer"><span><i className="footer-dot observed" /> Persisted backend event</span><span><i className="footer-line" /> Reference service boundary</span><span>Use Shift + mouse wheel or the scrollbar to explore the full diagram</span></div>
    </div>
  </div>
}

function healthClass(event) {
  if (!event) return 'idle'
  if (event.status === 'unavailable_fallback') return 'degraded'
  return 'observed'
}

function Inspector({ event, record, runId, onShowPayload }) {
  const timeline = Array.isArray(record?.timeline) ? record.timeline : []
  const decisionEvent = timeline.find((item) => item.stage === 'sql_decision')
  const bloomEvent = timeline.find((item) => item.stage === 'bloom_checked')
  if (!event) return <aside className="inspector panel"><div className="panel-title"><div><span className="kicker">RECORD INSPECTOR</span><h2>Waiting for selection</h2></div></div><div className="empty-state inspector-empty">Select an event or a moving record token to inspect its persisted details.</div></aside>
  return <aside className="inspector panel"><div className="panel-title"><div><span className="kicker">RECORD INSPECTOR</span><h2>{record?.lineNumber ? `Record #${record.lineNumber}` : 'Selected record'}</h2></div><span className={`inspector-pill ${eventTone(event)}`}>{statusLabel(record?.status || event.status)}</span></div>
    <div className="inspector-body"><div className="inspector-highlight"><span className={`big-status ${eventTone(event)}`} /><div><strong>{record?.status ? statusLabel(record.status) : statusLabel(event.status)}</strong><small>{nodeById[eventNode(event)]?.label || eventNode(event).replaceAll('_', ' ')}</small></div></div>
      <dl><dt>Record ID</dt><dd>{event.recordId || 'Not available'}</dd><dt>Line number</dt><dd>{record?.lineNumber ?? 'Not available'}</dd><dt>Test run ID</dt><dd>{event.testRunId || runId || 'Not available'}</dd><dt>Fingerprint</dt><dd className="fingerprint-value">{record?.fingerprint || event.detail?.fingerprint || 'Pending'}</dd><dt>Final SQL decision</dt><dd>{decisionEvent ? statusLabel(decisionEvent.status) : record?.status === 'accepted' || record?.status === 'duplicate' ? statusLabel(record.status) : 'Pending'}</dd><dt>Bloom result</dt><dd>{bloomEvent ? statusLabel(bloomEvent.status) : 'Not observed yet'}</dd><dt>Created / queued</dt><dd>{record?.createdAt ? `${formatTime(record.createdAt)} / ${formatTime(record.queuedAt)}` : 'Not available'}</dd><dt>Processed / duration</dt><dd>{record?.processedAt ? `${formatTime(record.processedAt)} / ${durationBetween(record.createdAt, record.processedAt) || 'Not available'}` : 'Pending'}</dd><dt>Error</dt><dd>{record?.errorMessage || 'None reported'}</dd></dl>
      {record?.payload !== undefined ? <div className="payload-panel"><span>Payload · explicitly requested</span><pre>{JSON.stringify(record.payload, null, 2)}</pre></div> : <button className="payload-button" disabled={!record} onClick={onShowPayload} type="button">{record ? 'Show payload for this record' : 'Loading record details…'}</button>}
      <div className="record-timeline"><strong>Persisted stage timeline</strong>{timeline.length ? timeline.map((item) => <div className="record-timeline-item" key={item.eventId}><span className={`timeline-mark ${eventTone(item)}`} /><div><span>{nodeById[eventNode(item)]?.label || eventNode(item).replaceAll('_', ' ')}</span><small>{statusLabel(item.status)} · {formatTime(item.occurredAt)}</small></div></div>) : <p>More stage events appear here as the worker processes this record.</p>}</div>
      <p className="inspector-note">Payload is omitted by default. Bloom results are hints; SQL decides accepted or duplicate.</p></div>
  </aside>
}

function RunsPage({ overview, onInspect }) {
  const runs = Array.isArray(overview?.recentRuns) ? overview.recentRuns : []
  return <section className="page-panel panel"><div className="panel-title"><div><span className="kicker">RUN HISTORY</span><h2>Recent test runs</h2><p className="panel-subtitle">Loaded from the backend overview endpoint.</p></div><span className="count">{runs.length}</span></div>{runs.length ? <div className="runs-table"><div className="runs-head"><span>Run ID</span><span>Status</span><span>Submitted</span><span>Accepted</span><span>Duplicates</span><span>Progress</span><span /></div>{runs.map((run) => <div className="runs-row" key={run.testRunId}><code>{run.testRunId}</code><span className={`run-state ${run.status}`}>{run.status}</span><span>{formatNumber(run.submittedCount)}</span><span>{formatNumber(run.acceptedCount)}</span><span>{formatNumber(run.duplicateCount)}</span><span>{formatNumber(run.progressPercent, '0')}%</span><button className="inspect-run" onClick={() => onInspect(run)} type="button">Inspect</button></div>)}</div> : <div className="empty-state">No recent runs are available yet. Submit a test run to populate this list.</div>}</section>
}

function RecordingsPage({ recordings, onReplay, onDelete }) {
  return <section className="page-panel panel recordings-page"><div className="panel-title"><div><span className="kicker">LOCAL RUN ARCHIVE</span><h2>Recorded processes</h2><p className="panel-subtitle">Runs are saved automatically in this browser as backend events arrive. Submitted payloads are not stored.</p></div><span className="count">{recordings.length}</span></div>
    {recordings.length ? <div className="recording-list">{recordings.map((recording) => {
      const status = recording.statusData || {}
      const eventCount = recording.events?.length || 0
      return <article className="recording-card" key={recording.testRunId}><div className="recording-main"><div className="recording-title"><span className={`recording-state ${recording.status}`}>{recording.status || 'saved'}</span><strong>{formatNumber(status.submittedCount ?? recording.expectedCount, '0')} records</strong><span>{formatNumber(eventCount, '0')} recorded events</span></div><code>{recording.testRunId}</code><small>Started {new Date(recording.createdAt).toLocaleString()} · Updated {new Date(recording.updatedAt || recording.createdAt).toLocaleString()}</small><div className="recording-counts"><span>Accepted <b>{formatNumber(status.acceptedCount, '0')}</b></span><span>Duplicates <b>{formatNumber(status.duplicateCount, '0')}</b></span><span>Failed <b>{formatNumber(status.failedCount, '0')}</b></span></div></div><div className="recording-actions"><button className="replay-button" disabled={!eventCount} onClick={() => onReplay(recording)} type="button">Replay events <span>▶</span></button><button className="delete-recording" onClick={() => onDelete(recording.testRunId)} type="button">Delete local copy</button></div></article>
    })}</div> : <div className="empty-state recording-empty">No runs have been recorded in this browser yet. Start a pipeline run and the event stream will be saved automatically.</div>}
  </section>
}

function ReplayControls({ recording, current, total, playing, speed, onToggle, onRestart, onSeek, onSpeedChange, onExit }) {
  const isPartial = recording.status !== 'completed'
  return <section className="replay-controls panel" aria-label="Recorded run playback">
    <div className="replay-heading"><span className="replay-icon"><Icon name="record" /></span><div><span className="kicker">{isPartial ? 'PARTIAL RECORDING' : 'RECORDED RUN REPLAY'}</span><strong>{playing ? 'Replaying observed events' : current >= total ? 'Replay complete' : 'Replay paused'}</strong><small>Run {recording.testRunId}</small></div></div>
    <div className="replay-progress"><div className="replay-progress-label"><span>{current} / {total} events</span><span>{isPartial ? 'Recording ended before backend completion' : 'Saved event order'}</span></div><input aria-label="Seek through recorded events" max={Math.max(total, 1)} min="0" onChange={(event) => onSeek(event.target.value)} type="range" value={current} /><div className="replay-buttons"><button className="replay-button" onClick={onToggle} type="button">{playing ? 'Pause' : current >= total ? 'Play again' : 'Resume'}</button><button className="secondary-button" onClick={onRestart} type="button">Restart</button><label className="replay-speed">Speed<select aria-label="Replay speed" onChange={(event) => onSpeedChange(event.target.value)} value={speed}><option value="0.5">0.5×</option><option value="1">1×</option><option value="2">2×</option><option value="4">4×</option></select></label></div></div>
    <button className="exit-replay" onClick={onExit} type="button">Exit replay</button>
  </section>
}

function Monitoring({ health, overview, onRefresh }) {
  const dependencies = health?.dependencies || overview || {}
  const items = [
    ['sql', 'Azure SQL', 'Final decision store'],
    ['redis', 'Managed Redis', 'Bloom pre-check hint'],
    ['serviceBus', 'Service Bus', 'Asynchronous work queue'],
  ]
  return <div className="monitor-grid"><section className="panel monitor-hero"><div className="panel-title"><div><span className="kicker">SYSTEM MONITORING</span><h2>Dependency readiness</h2><p className="panel-subtitle">Health endpoint refreshes every 10 seconds. Redis can degrade safely while SQL and Service Bus remain ready.</p></div><button className="secondary-button" onClick={onRefresh} type="button">Refresh now</button></div><div className="dependency-grid">{items.map(([key, label, detail]) => { const tone = dependencyTone(dependencies, key); return <article className={`dependency ${tone}`} key={key}><span className="dependency-icon"><Icon name={key === 'sql' ? 'sql' : key === 'redis' ? 'redis' : 'queue'} /></span><div><span>{label}</span><strong>{tone === 'healthy' ? 'Healthy' : tone === 'degraded' ? 'Degraded fallback' : 'Unavailable'}</strong><small>{dependencies[key]?.fallback || dependencies[key]?.role || dependencies[key]?.note || detail}</small>{key === 'serviceBus' && dependencies[key]?.active !== undefined && <small>Active {formatNumber(dependencies[key].active)} · Dead-lettered {formatNumber(dependencies[key].deadLettered)} · Scheduled {formatNumber(dependencies[key].scheduled)}</small>}</div></article>})}</div><div className="monitor-footnote">Azure Monitor is shown as a platform integration point in the pipeline. This deployment does not expose Application Insights or Log Analytics metrics through this UI.</div></section><section className="panel monitor-hero overview-panel"><span className="kicker">OVERVIEW SNAPSHOT</span><h2>Recent activity</h2><div className="overview-quick-stats"><div><strong>{formatNumber(overview?.runsLastHour)}</strong><span>runs last hour</span></div><div><strong>{formatNumber(overview?.lastHour?.accepted)}</strong><span>accepted</span></div><div><strong>{formatNumber(overview?.lastHour?.duplicates)}</strong><span>duplicates</span></div><div><strong>{formatNumber(overview?.serviceBus?.active)}</strong><span>active messages</span></div></div><div className="overview-recent"><strong>Backend response detail</strong><pre>{overview ? JSON.stringify(overview, null, 2) : 'No overview returned yet.'}</pre></div></section></div>
}

export default App
