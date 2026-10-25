import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const repositoryName = process.env.GITHUB_REPOSITORY?.split('/').at(-1)
const isUserOrOrganizationSite = repositoryName?.endsWith('.github.io')
const base = process.env.GITHUB_PAGES === 'true' && repositoryName && !isUserOrOrganizationSite
  ? `/${repositoryName}/`
  : '/'

export default defineConfig({
  base,
  plugins: [tailwindcss(), react()],
  server: {
    proxy: {
      '/bff': 'http://localhost:8787',
    },
  },
})
