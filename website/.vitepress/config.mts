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
    text: 'FAQ',
    collapsed: true,
    items: [{ text: 'Why not Apache Fluss', link: '/docs/faq/fluss' }],
  },
  {
    text: 'Design',
    collapsed: true,
    items: [
      { text: 'Overview', link: '/docs/design/overview' },
      { text: 'Tables', link: '/docs/design/tables' },
      { text: 'Metadata', link: '/docs/design/metadata' },
      { text: 'Log tablets', link: '/docs/design/log-tablets' },
      { text: 'KV tablets', link: '/docs/design/kv-tablets' },
      { text: 'Writes', link: '/docs/design/writes' },
      { text: 'Reads', link: '/docs/design/reads' },
      { text: 'Tiering', link: '/docs/design/tiering' },
        { text: 'Union read', link: '/docs/design/union-read' },
        { text: 'SQL', link: '/docs/design/query' },
        { text: 'Ownership & failover', link: '/docs/design/ownership' },
      { text: 'Retention', link: '/docs/design/retention' },
      { text: 'Protocols', link: '/docs/design/protocols' },
    ],
  },
  {
    text: 'Operations',
    collapsed: true,
    items: [
      { text: 'CLI', link: '/docs/operations/cli' },
      { text: 'Configuration', link: '/docs/operations/configuration' },
      {
        text: 'Deployment',
        collapsed: true,
        items: [{ text: 'Docker', link: '/docs/operations/deployment/docker' }],
      },
      { text: 'Tuning', link: '/docs/operations/tuning' },
    ],
  },
  // {
  //   text: 'API reference',
  //   collapsed: true,
  //   items: [
  //     { text: 'Arrow Flight', link: '/docs/flight' },
  //     { text: 'Kafka protocol', link: '/docs/kafka' },
  //     { text: 'Flight SQL', link: '/docs/flight-sql' },
  //     { text: 'Table options', link: '/docs/options' },
  //     { text: 'Rust client', link: '/docs/client/rust' },
  //   ],
  // },
  {
    text: 'Community',
    collapsed: true,
    items: [
      { text: 'Contribute', link: '/docs/contribute' },
      { text: 'Acknowledgements', link: '/docs/acknowledgements' },
    ],
  },
];

export default defineConfig({
  title: 'Mink',
  description:
    'Lakehouse-native streaming storage on object storage. Kafka and Arrow Flight in, open table formats out, hot and cold served as one table.',
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
      { text: 'GitHub', link: 'https://github.com/addu390/mink' },
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
