import { defineConfig } from 'vitepress';
import { tabsMarkdownPlugin } from 'vitepress-plugin-tabs';

const docsSidebar = [
  {
    text: 'Getting started',
    collapsed: false,
    items: [
      { text: 'Introduction', link: '/docs/' },
      { text: 'Quick start', link: '/docs/quick-start' },
    ],
  },
  {
    text: 'Why not',
    collapsed: false,
    items: [
      { text: 'Apache Fluss', link: '/docs/why-not/fluss' },
      { text: 'Table topics', link: '/docs/why-not/table-topics' },
      { text: 'Connectors', link: '/docs/why-not/connectors' },
    ],
  },
  {
    text: 'Design',
    collapsed: false,
    items: [
      { text: 'Overview', link: '/docs/design/overview' },
      { text: 'Tables', link: '/docs/design/tables' },
      { text: 'Indexing', link: '/docs/design/indexing' },
      { text: 'Snapshots', link: '/docs/design/snapshots' },
      { text: 'Reads', link: '/docs/design/reads' },
      { text: 'Layout and pruning', link: '/docs/design/layout' },
      { text: 'Retention', link: '/docs/design/retention' },
      { text: 'Cluster', link: '/docs/design/cluster' },
    ],
  },
  {
    text: 'Using Mink',
    collapsed: false,
    items: [
      { text: 'Engines', link: '/docs/engines' },
      { text: 'File formats', link: '/docs/formats' },
    ],
  },
  {
    text: 'Operations',
    collapsed: false,
    items: [
      { text: 'Deployment', link: '/docs/operations/deployment' },
      { text: 'CLI', link: '/docs/operations/cli' },
      { text: 'Configuration', link: '/docs/operations/configuration' },
      { text: 'Console', link: '/docs/operations/console' },
      { text: 'Admin API', link: '/docs/operations/admin' },
      { text: 'Metrics', link: '/docs/operations/metrics' },
      { text: 'Tuning', link: '/docs/operations/tuning' },
    ],
  },
  {
    text: 'Community',
    collapsed: true,
    items: [
      { text: 'Roadmap', link: '/docs/roadmap' },
      { text: 'Contribute', link: '/docs/contribute' },
    ],
  },
];

export default defineConfig({
  title: 'Mink',
  description:
    'Kafka topics as Iceberg tables: a read-only Iceberg REST catalog and S3 endpoint over any Kafka cluster.',
  base: process.env.BASE_PATH ?? '/',
  cleanUrls: true,
  appearance: false,
  srcDir: 'pages',
  vite: {
    publicDir: 'assets',
    server: {
      fs: {
        allow: ['..'],
      },
    },
  },
  markdown: {
    theme: 'github-light',
    config(md) {
      md.use(tabsMarkdownPlugin);
    },
  },
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/images/logo.svg' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    [
      'link',
      { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
    ],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Marcellus&family=Playfair+Display:wght@400;500;600&display=swap',
      },
    ],
  ],
  themeConfig: {
    siteTitle: false,
    nav: [
      { text: 'Docs', link: '/docs' },
      { text: 'Contribute', link: '/docs/contribute' },
      { text: 'GitHub', link: 'https://github.com/PicoMQ/mink' },
    ],
    sidebar: docsSidebar,
    search: {
      provider: 'local',
    },
    outline: {
      level: [1, 3],
      label: 'On this page',
    },
    footer: {
      copyright: '© 2026 Mink. Apache 2.0.',
    },
  },
});
