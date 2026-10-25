# Azure Deduplication Virtual Lab UI

React + Vite interface for submitting records to the Azure Functions deduplication backend, watching persisted processing events, inspecting records, and replaying saved runs.

## Run locally

From this directory, copy `.env.example` to `.env.local` and set the deployed Function host and its Function key in `.env.local`:

```text
AZURE_FUNCTION_BASE_URL=https://<function-app-hostname>
AZURE_FUNCTION_KEY=<function-key>
PORT=8787
WEB_ORIGINS=https://<github-owner>.github.io
```

Keep the Function key only in `.env.local` or the server host's secret settings. Never put it in a `VITE_` variable or commit it.

```bash
npm install
npm run dev
```

Open the Vite URL printed in the terminal. The browser calls `/bff`; the Express proxy reads `.env.local` and adds the Function key on the server. Restart the local server after changing `.env.local`.

## Deploy the UI to GitHub Pages

The repository workflow builds this directory and publishes `deduplication/dist` to GitHub Pages when `main` is updated. In the GitHub repository, open **Settings → Pages** and select **GitHub Actions** as the build and deployment source.

GitHub Pages serves static files; it does not run the Express proxy. To use the Azure-backed features from the deployed UI:

1. Deploy `server/index.mjs` as a separate HTTPS Node service (for example, an Azure App Service) with `npm start` as its start command.
2. Configure these server-side environment values on that service: `AZURE_FUNCTION_BASE_URL`, `AZURE_FUNCTION_KEY`, and `WEB_ORIGINS`. Set `WEB_ORIGINS` to the exact Pages origin, such as `https://<github-owner>.github.io` (no path or trailing slash). Keep the Function key in the server's secret configuration.
3. In the GitHub repository, add the Actions **repository variable** `VITE_API_PROXY_URL` with the public BFF URL ending in `/bff`, for example `https://<bff-host>/bff`. It is a public URL, not a secret.
4. Re-run the **Deploy deduplication UI to GitHub Pages** workflow so the build includes that URL.

If `VITE_API_PROXY_URL` is empty, the static UI still loads and explains that a backend proxy must be configured; it does not try to use a non-existent `/bff` route on GitHub Pages. The Function key is never shipped to the browser.

### Push from PowerShell

Run these from the repository root. Replace the owner and repository placeholders if no `origin` remote is configured:

```powershell
cd "D:\ALL Projects\College Mini-Projects\az-2"
git status
git add -A
git status --short
git commit -m "Prepare deduplication lab for GitHub Pages"
git branch -M main
git remote -v
# If origin is missing:
git remote add origin https://github.com/<OWNER>/<REPOSITORY>.git
git push -u origin main
```

If `origin` already points to the correct GitHub repository, skip `git remote add`. Review `git status --short` before committing and confirm `.env.local` is not listed. If you need to replace the URL for an existing `origin`, use `git remote set-url origin https://github.com/<OWNER>/<REPOSITORY>.git`.

## UI features

- Submit UTF-8 TXT files or JSON records, paste JSON, or load a duplicate sample.
- Split inputs into batches of 100 and retry a failed batch with its original run ID and line numbers.
- Animate records from persisted backend events; Azure SQL is authoritative and Redis Bloom is only a hint.
- Inspect event timelines and record metadata. Payload content stays hidden unless explicitly requested.
- Save observed run events in this browser's IndexedDB. Open **Recordings** to replay, pause, change speed, seek, restart, or delete a local recording.

Recordings contain run counters and event metadata, not submitted payloads. They stay in the current browser profile and are not uploaded to another service. The Functions backend also retains its own event history.

## Scripts

```bash
npm run dev      # Vite frontend + local API proxy
npm run build    # Production frontend build
npm run preview  # Preview the production build
```
