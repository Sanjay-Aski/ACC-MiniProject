const DATABASE_NAME = 'azure-dedup-virtual-lab'
const STORE_NAME = 'recordings'
const DATABASE_VERSION = 1

function openDatabase() {
  if (!globalThis.indexedDB) return Promise.reject(new Error('This browser does not support local run recordings.'))
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DATABASE_NAME, DATABASE_VERSION)
    request.onupgradeneeded = () => {
      const database = request.result
      if (!database.objectStoreNames.contains(STORE_NAME)) database.createObjectStore(STORE_NAME, { keyPath: 'testRunId' })
    }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error || new Error('Could not open local run recordings.'))
    request.onblocked = () => reject(new Error('Local recordings are blocked by another open tab. Close the other tab and retry.'))
  })
}

async function runRequest(mode, action) {
  const database = await openDatabase()
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(STORE_NAME, mode)
    const store = transaction.objectStore(STORE_NAME)
    let result
    try {
      const request = action(store)
      request.onsuccess = () => { result = request.result }
      request.onerror = () => reject(request.error || new Error('Local recording operation failed.'))
    } catch (error) {
      reject(error)
    }
    transaction.oncomplete = () => { database.close(); resolve(result) }
    transaction.onerror = () => { database.close(); reject(transaction.error || new Error('Local recording transaction failed.')) }
    transaction.onabort = () => { database.close(); reject(transaction.error || new Error('Local recording transaction was cancelled.')) }
  })
}

export function saveRecording(recording) {
  return runRequest('readwrite', (store) => store.put(recording))
}

export async function listRecordings() {
  const recordings = await runRequest('readonly', (store) => store.getAll())
  return (recordings || []).sort((a, b) => new Date(b.updatedAt || b.createdAt).getTime() - new Date(a.updatedAt || a.createdAt).getTime())
}

export function deleteRecording(testRunId) {
  return runRequest('readwrite', (store) => store.delete(testRunId))
}
