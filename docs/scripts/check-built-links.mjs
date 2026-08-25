import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { dirname, extname, join, normalize, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const docsDir = resolve(scriptDir, '..')
const distDir = join(docsDir, '.vitepress', 'dist')

if (!existsSync(distDir)) {
  throw new Error(`VitePress output not found at ${distDir}; run npm run build first`)
}

function walk(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name)
    return entry.isDirectory() ? walk(path) : [path]
  })
}

function decodePath(value) {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

function candidates(htmlFile, rawTarget) {
  const target = decodePath(rawTarget.split('#', 1)[0].split('?', 1)[0])
  if (!target) return []

  const absolute = target.startsWith('/')
    ? resolve(distDir, `.${target}`)
    : resolve(dirname(htmlFile), target)

  const safeRelative = relative(distDir, absolute)
  if (safeRelative.startsWith(`..${sep}`) || safeRelative === '..') return []

  if (target.endsWith('/')) return [join(absolute, 'index.html')]
  if (extname(absolute)) return [absolute]
  return [`${absolute}.html`, join(absolute, 'index.html')]
}

const htmlFiles = walk(distDir).filter((file) => file.endsWith('.html'))
const failures = []
const attributePattern = /(?:href|src)=["']([^"']+)["']/g

for (const htmlFile of htmlFiles) {
  const html = readFileSync(htmlFile, 'utf8')
  for (const match of html.matchAll(attributePattern)) {
    const target = match[1]
    if (
      target.startsWith('#') ||
      target.startsWith('//') ||
      /^(?:https?:|mailto:|tel:|data:|javascript:)/i.test(target)
    ) {
      continue
    }

    const paths = candidates(htmlFile, target)
    if (paths.length > 0 && !paths.some(existsSync)) {
      failures.push(`${normalize(relative(distDir, htmlFile))} -> ${target}`)
    }
  }
}

if (failures.length > 0) {
  console.error(`Found ${failures.length} broken built link(s):`)
  for (const failure of [...new Set(failures)].sort()) console.error(`- ${failure}`)
  process.exitCode = 1
} else {
  console.log(`Verified ${htmlFiles.length} generated HTML pages with no broken local links.`)
}
