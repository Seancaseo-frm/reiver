import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Reiver',
  description: 'Full-stack observability platform with a built-in data warehouse, prompt hub, and LLM gateway.',
  sitemap: {
    hostname: 'https://docs.reiver.ai',
  },

  srcExclude: [
    'DESIGN.md',
    'FEATURE_DESIGN_DOC.md',
    'GAME_SEMANTIC_CONVENTIONS.md',
    'invariants.md',
    'roadmap.md',
    'investor-conversation-learnings.md',
    'openclaw-guide.md',
    'okta-*.md',
    'udf-*.md',
    'exception-*.md',
    'pitch-deck.html',
    'data_sources/**',
  ],

  head: [
    ['link', { rel: 'icon', href: '/favicon.ico' }],
  ],

  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'Reiver Docs',

    nav: [
      { text: 'Flow', link: '/flow/getting-started' },
      { text: 'Watch', link: '/watch/' },
      // Pond disabled — re-enable when Pond launches
      // { text: 'Pond', link: '/pond/' },
      { text: 'AI Agent', link: '/agent/' },
      { text: 'SDKs', link: '/sdks/' },
      { text: 'Legal', link: '/legal/privacy' },
    ],

    sidebar: {
      '/flow/': [
        {
          text: 'Flow — Prompt Hub & LLM Gateway',
          items: [
            { text: 'Getting Started', link: '/flow/getting-started' },
            { text: 'Prompt Management', link: '/flow/prompt-management' },
            { text: 'Features', link: '/flow/features' },
            { text: 'Routing', link: '/flow/routing' },
            { text: 'Session Telemetry', link: '/flow/session-telemetry' },
            { text: 'Supported Models', link: '/flow/models' },
            { text: 'API Reference', link: '/flow/api-reference' },
            { text: 'Management API', link: '/flow/management-api' },
          ],
        },
      ],
      '/watch/': [
        {
          text: 'Watch — APM',
          items: [
            { text: 'Overview', link: '/watch/' },
          ],
        },
      ],
      // Pond disabled — re-enable when Pond launches
      // '/pond/': [
      //   {
      //     text: 'Pond — Data Warehouse',
      //     items: [
      //       { text: 'Overview', link: '/pond/' },
      //     ],
      //   },
      // ],
      '/agent/': [
        {
          text: 'AI Agent',
          items: [
            { text: 'Overview', link: '/agent/' },
            { text: 'MCP Setup', link: '/agent/mcp-setup' },
            { text: 'Available Tools', link: '/agent/tools' },
            { text: 'In-App Agent', link: '/agent/in-app' },
          ],
        },
      ],
      '/sdks/': [
        {
          text: 'SDKs',
          items: [
            { text: 'Overview', link: '/sdks/' },
          ],
        },
      ],
      '/legal/': [
        {
          text: 'Legal',
          items: [
            { text: 'Terms of Service', link: '/legal/terms' },
            { text: 'Privacy Policy', link: '/legal/privacy' },
            { text: 'Support', link: '/legal/support' },
          ],
        },
      ],
    },

    search: {
      provider: 'local',
    },

    footer: {
      message: '<a href="/legal/terms">Terms of Service</a> · <a href="/legal/privacy">Privacy Policy</a> · <a href="/legal/support">Support</a>',
      copyright: 'Reiver Documentation',
    },
  },
})
