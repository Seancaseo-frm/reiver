import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Reiver · Production AI control plane for LLM observability',
  description: 'Reiver is a production AI control plane - OpenTelemetry-native observability, LLM gateway, and prompt hub in one.',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}

