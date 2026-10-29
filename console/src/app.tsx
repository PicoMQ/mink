import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/preact-query'
import { BookOpen, Database, FileCode, LayoutDashboard, Plug, Server, SquareTerminal } from 'lucide-preact'
import { LocationProvider, Route, Router, lazy, useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { ApiError } from './api/client'
import { cluster, NO_SQL } from './api/cluster'
import { Logo } from './components/logo'
import { current } from './location'

import './app.css'

const DOCS = 'https://github.com/PicoMQ/mink'

const SECTIONS = [
  { href: '/', label: 'Overview', icon: LayoutDashboard },
  { href: '/tables', label: 'Tables', icon: Database },
  { href: '/nodes', label: 'Nodes', icon: Server },
  { href: '/registry', label: 'Registry', icon: FileCode },
  { href: '/sql', label: 'SQL', icon: SquareTerminal },
  { href: '/connect', label: 'Connect', icon: Plug },
]

const Overview = lazy(() => import('./views/overview'))
const Tables = lazy(() => import('./views/tables'))
const TableView = lazy(() => import('./views/table'))
const FileView = lazy(() => import('./views/file'))
const SnapshotView = lazy(() => import('./views/snapshot'))
const Nodes = lazy(() => import('./views/nodes'))
const NodeView = lazy(() => import('./views/node'))
const Registry = lazy(() => import('./views/registry'))
const Definition = lazy(() => import('./views/definition'))
const SqlView = lazy(() => import('./views/sql'))
const Connect = lazy(() => import('./views/connect'))
const Missing = lazy(() => import('./views/missing'))

export function App() {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            refetchOnWindowFocus: false,
            retry: (failures, error) => !(error instanceof ApiError) && failures < 2,
          },
        },
      }),
  )

  return (
    <QueryClientProvider client={client}>
      <LocationProvider>
        <div class="app">
          <Header />
          <Navigation />
          <main class="app-main">
            <Router>
              <Route path="/" component={Overview} />
              <Route path="/tables" component={Tables} />
              <Route path="/tables/:name/files/:file+" component={FileView} />
              <Route path="/tables/:name/snapshots/:id" component={SnapshotView} />
              <Route path="/tables/:name" component={TableView} />
              <Route path="/tables/:name/:tab" component={TableView} />
              <Route path="/nodes" component={Nodes} />
              <Route path="/nodes/:id" component={NodeView} />
              <Route path="/registry" component={Registry} />
              <Route path="/registry/:id" component={Definition} />
              <Route path="/sql" component={SqlView} />
              <Route path="/connect" component={Connect} />
              <Route default component={Missing} />
            </Router>
          </main>
        </div>
      </LocationProvider>
    </QueryClientProvider>
  )
}

function Header() {
  return (
    <header class="app-header">
      <a class="app-brand" href="/">
        <Logo />
        Mink
      </a>
      <span class="app-title">Admin console</span>
    </header>
  )
}

function Navigation() {
  const { path } = useLocation()
  const active = current(path, SECTIONS.map((section) => section.href))
  const state = useQuery(cluster())
  const unavailable = state.data?.sql === null

  return (
    <nav class="app-nav" aria-label="Sections">
      {SECTIONS.map(({ href, label, icon: Icon }) =>
        href === '/sql' && unavailable ? (
          <span key={href} class="app-section" aria-disabled="true" title={NO_SQL}>
            <Icon size={18} />
            {label}
          </span>
        ) : (
          <a key={href} class="app-section" href={href} aria-current={href === active ? 'page' : undefined}>
            <Icon size={18} />
            {label}
          </a>
        ),
      )}
      <div class="app-footer">
        <a class="app-section" href={DOCS} target="_blank" rel="noreferrer">
          <BookOpen size={18} />
          Documentation
        </a>
      </div>
    </nav>
  )
}
