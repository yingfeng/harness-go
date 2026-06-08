import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Open Agent Builder',
  description: 'Visual AI Agent Workflow Builder powered by Harness-Go',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
