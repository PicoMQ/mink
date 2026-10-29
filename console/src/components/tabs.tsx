import { useLocation } from 'preact-iso'

import { current } from '../location'

import './tabs.css'

interface Tab {
  path?: string
  label: string
}

export function Tabs({ label, tabs, href }: { label: string; tabs: Tab[]; href: (path: string | undefined) => string }) {
  const { path } = useLocation()
  const links = tabs.map((tab) => ({ href: href(tab.path), label: tab.label }))
  const active = current(path, links.map((link) => link.href))

  return (
    <nav class="tabs" aria-label={label}>
      {links.map((link) => (
        <a key={link.href} class="tabs-item" href={link.href} aria-current={link.href === active ? 'page' : undefined}>
          {link.label}
        </a>
      ))}
    </nav>
  )
}
