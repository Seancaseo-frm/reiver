import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const mcpDir = resolve(scriptDir, '..')
const docsSource = readFileSync(join(mcpDir, 'src', 'docs.rs'), 'utf8')
const pagePattern = /doc_page!\(\s*"(agent:\/\/[^"\s]+)"[\s\S]*?"(\.\.\/agent-docs\/[^"\s]+\.md)"\s*\)/g
const pages = [...docsSource.matchAll(pagePattern)].map((match) => ({
  uri: match[1],
  file: resolve(mcpDir, 'src', match[2]),
}))

const failures = []
const seen = new Set()

if (pages[0]?.uri !== 'agent://onboarding') {
  failures.push('agent://onboarding is not the first registered resource')
}

for (const page of pages) {
  if (seen.has(page.uri)) failures.push(`duplicate resource URI: ${page.uri}`)
  seen.add(page.uri)
  if (!existsSync(page.file)) failures.push(`missing resource file: ${page.uri}`)
}

const agentFiles = readdirSync(join(mcpDir, 'agent-docs'))
  .filter((file) => file.endsWith('.md'))
  .map((file) => resolve(mcpDir, 'agent-docs', file))

for (const file of agentFiles) {
  const content = readFileSync(file, 'utf8')
  for (const match of content.matchAll(/agent:\/\/[A-Za-z0-9_./-]+/g)) {
    if (!seen.has(match[0])) {
      failures.push(`${relative(mcpDir, file)} references unknown resource ${match[0]}`)
    }
  }
}

if (pages.length !== agentFiles.length) {
  failures.push(
    `${pages.length} registered resources but ${agentFiles.length} agent documentation files`,
  )
}

const onboarding = readFileSync(join(mcpDir, 'agent-docs', 'onboarding.md'), 'utf8')
const watch = readFileSync(join(mcpDir, 'agent-docs', 'watch-overview.md'), 'utf8')
const sessionContract = readFileSync(
  join(mcpDir, 'agent-docs', 'flow-session-telemetry.md'),
  'utf8',
)
const toolReference = readFileSync(join(mcpDir, 'agent-docs', 'agent-tools.md'), 'utf8')
for (const signal of ['traces', 'logs', 'metrics']) {
  if (!onboarding.includes(signal)) failures.push(`onboarding omits ${signal}`)
  if (!watch.includes(signal)) failures.push(`Watch resource omits ${signal}`)
}

for (const requirement of [
  '**My understanding**',
  'gateway_settings.agent_soul',
  'Select the onboarding track',
  'independently valid outcomes',
  'Session and Identity Contract',
  'business outcome',
  'hard technical boundary',
  'without repeatedly asking for approval',
  'agent_soul',
  'session_labels',
  'synthetic sessions',
  'rollback path',
]) {
  if (!onboarding.includes(requirement)) {
    failures.push(`onboarding omits required business/autonomy marker: ${requirement}`)
  }
}

for (const requirement of [
  'Session and Identity Contract',
  'no separate Reiver session-start call',
  '30-minute idle evaluator',
  'stable pseudonymous',
  'Anonymous-user policy',
  'Tenant scoping',
]) {
  if (!sessionContract.includes(requirement)) {
    failures.push(`session contract resource omits ${requirement}`)
  }
}

const instructionsMatch = docsSource.match(/SERVER_INSTRUCTIONS:\s*&str\s*=\s*"([^"]+)";/)
if (!instructionsMatch) {
  failures.push('could not read SERVER_INSTRUCTIONS')
} else {
  const instructions = instructionsMatch[1]
  if (Buffer.byteLength(instructions, 'utf8') > 512) {
    failures.push(`SERVER_INSTRUCTIONS is ${Buffer.byteLength(instructions, 'utf8')} bytes (max 512)`)
  }
  for (const marker of [
    'agent://onboarding',
    'business context',
    'hard boundary',
    'selected Flow, Watch, or Complete track',
    'Session and Identity Contract',
  ]) {
    if (!instructions.includes(marker)) failures.push(`SERVER_INSTRUCTIONS omits ${marker}`)
  }
}

for (const facade of ['search', 'get', 'list', 'analyze']) {
  const code = readFileSync(join(mcpDir, 'src', 'actions', 'facade', `${facade}.rs`), 'utf8')
  const discriminators = [...code.matchAll(/#\[serde\(rename = "([^"]+)"\)\]/g)].map(
    (match) => match[1],
  )
  for (const discriminator of discriminators) {
    if (!toolReference.includes(`\`${discriminator}\``)) {
      failures.push(`agent-tools.md omits ${facade} discriminator ${discriminator}`)
    }
  }
}

const executeCode = readFileSync(
  join(mcpDir, 'src', 'actions', 'facade', 'execute_action.rs'),
  'utf8',
)
const executePairs = [
  ...executeCode.matchAll(/\("([a-z_]+)", "([a-z_]+)"\)\s*=>/g),
].map((match) => ({ resource: match[1], action: match[2] }))

for (const { resource, action } of executePairs) {
  const line = toolReference
    .split('\n')
    .find((candidate) => candidate.startsWith(`- \`${resource}\` — `))
  if (!line) {
    failures.push(`agent-tools.md omits execute resource ${resource}`)
  } else if (!line.includes(action)) {
    failures.push(`agent-tools.md omits execute pair ${resource}/${action}`)
  }
}

if (failures.length > 0) {
  console.error(`Found ${failures.length} MCP documentation problem(s):`)
  for (const failure of failures) console.error(`- ${failure}`)
  process.exitCode = 1
} else {
  console.log(
    `Verified ${pages.length} MCP resources, agent:// references, and facade discriminators.`,
  )
}
