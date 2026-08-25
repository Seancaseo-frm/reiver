import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import { dirname, extname, join, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const docsDir = resolve(scriptDir, '..')
const publicDir = join(docsDir, 'public')

function markdownFiles(path) {
  if (!existsSync(path)) return []
  if (!statSync(path).isDirectory()) return [path]
  return readdirSync(path, { withFileTypes: true }).flatMap((entry) => {
    const child = join(path, entry.name)
    return entry.isDirectory() ? markdownFiles(child) : child.endsWith('.md') ? [child] : []
  })
}

const pages = [
  join(docsDir, 'index.md'),
  join(docsDir, 'quickstart.md'),
  ...['agent', 'flow', 'legal', 'sdks', 'watch'].flatMap((dir) =>
    markdownFiles(join(docsDir, dir)),
  ),
]

function candidates(source, rawTarget) {
  const cleanTarget = rawTarget.replace(/^<|>$/g, '').split('#', 1)[0].split('?', 1)[0]
  if (!cleanTarget) return []

  if (cleanTarget.startsWith('/')) {
    const publicAsset = resolve(publicDir, `.${cleanTarget}`)
    const page = resolve(docsDir, `.${cleanTarget}`)
    if (extname(cleanTarget)) return [publicAsset, page]
    if (cleanTarget.endsWith('/')) return [join(page, 'index.md')]
    return [`${page}.md`, join(page, 'index.md')]
  }

  const page = resolve(dirname(source), cleanTarget)
  if (extname(page)) return [page]
  if (cleanTarget.endsWith('/')) return [join(page, 'index.md')]
  return [`${page}.md`, join(page, 'index.md')]
}

function isLocalTarget(target) {
  return !(
    target.startsWith('#') ||
    target.startsWith('//') ||
    /^(?:https?:|mailto:|tel:|data:|javascript:)/i.test(target)
  )
}

const failures = []

const robots = readFileSync(join(publicDir, 'robots.txt'), 'utf8')
for (const marker of ['Allow: /', 'ClaudeBot', 'https://docs.reiver.ai/sitemap.xml']) {
  if (!robots.includes(marker)) failures.push(`docs/public/robots.txt omits ${marker}`)
}

const docsFullGuide = readFileSync(join(publicDir, 'llms-full.txt'), 'utf8')
const websiteFullGuide = readFileSync(
  resolve(docsDir, '..', 'website', 'frontend', 'public', 'llms-full.txt'),
  'utf8',
)
if (docsFullGuide !== websiteFullGuide) {
  failures.push('docs/public/llms-full.txt differs from website/frontend/public/llms-full.txt')
}

for (const shortGuide of [
  readFileSync(join(publicDir, 'llms.txt'), 'utf8'),
  readFileSync(resolve(docsDir, '..', 'website', 'frontend', 'public', 'llms.txt'), 'utf8'),
]) {
  for (const marker of [
    'agent://onboarding',
    'business outcomes',
    'hard technical boundary',
    'independently completable',
    'Session and Identity Contract',
  ]) {
    if (!shortGuide.includes(marker)) failures.push(`llms.txt omits ${marker}`)
  }
}

const machineGuide = readFileSync(join(publicDir, 'llms-full.txt'), 'utf8')
for (const marker of [
  'agent://onboarding',
  'Business context first',
  'Select the onboarding track',
  'Session and Identity Contract',
  '30-minute',
  'stable pseudonymous',
  'gateway_settings',
  'Delegated autonomy',
  'hard technical boundary',
  'Business-aware activation',
  'agent_soul',
  'application trace',
  'structured log',
  'runtime metric',
]) {
  if (!machineGuide.includes(marker)) failures.push(`llms-full.txt omits ${marker}`)
}

const quickstart = readFileSync(join(docsDir, 'quickstart.md'), 'utf8')
for (const marker of ['Flow + Prompt Hub', '**Watch**', '**Complete Reiver**']) {
  if (!quickstart.includes(marker)) failures.push(`quickstart.md omits track ${marker}`)
}

const sessionContract = readFileSync(join(docsDir, 'flow', 'session-telemetry.md'), 'utf8')
for (const marker of [
  'Session and Identity Contract',
  'does not require a separate session-start endpoint',
  '30 minutes',
  'stable pseudonymous',
  'Anonymous-user policy',
  'Tenant scoping',
  'gateway_settings.agent_soul',
]) {
  if (!sessionContract.includes(marker)) failures.push(`session contract omits ${marker}`)
}

const markdownPattern = /!?(?:\[[^\]]*\])\(([^\s)]+)(?:\s+["'][^"']*["'])?\)/g
const htmlPattern = /(?:href|src)=["']([^"']+)["']/g

for (const page of pages) {
  const source = readFileSync(page, 'utf8')
    .replace(/```[\s\S]*?```/g, '')
    .replace(/`[^`\n]*`/g, '')
  const targets = [
    ...[...source.matchAll(markdownPattern)].map((match) => match[1]),
    ...[...source.matchAll(htmlPattern)].map((match) => match[1]),
  ]

  for (const target of targets) {
    if (!isLocalTarget(target)) continue
    const paths = candidates(page, target)
    const escaped = paths.every((path) => {
      const rel = relative(docsDir, path)
      return rel === '..' || rel.startsWith(`..${sep}`)
    })
    if (!escaped && paths.length > 0 && !paths.some(existsSync)) {
      failures.push(`${relative(docsDir, page)} -> ${target}`)
    }
  }
}

// Navigation and sidebar links live in the VitePress config rather than Markdown.
const configPath = join(docsDir, '.vitepress', 'config.ts')
const config = readFileSync(configPath, 'utf8')
for (const match of config.matchAll(/\blink:\s*['"]([^'"]+)['"]/g)) {
  const target = match[1]
  if (!isLocalTarget(target)) continue
  const paths = candidates(join(docsDir, 'index.md'), target)
  if (paths.length > 0 && !paths.some(existsSync)) {
    failures.push(`.vitepress/config.ts -> ${target}`)
  }
}

if (failures.length > 0) {
  console.error(`Found ${failures.length} broken documentation source link(s):`)
  for (const failure of [...new Set(failures)].sort()) console.error(`- ${failure}`)
  process.exitCode = 1
} else {
  console.log(`Verified ${pages.length} published Markdown pages with no broken local links.`)
}
