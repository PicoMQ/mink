import { defaultKeymap, history, historyKeymap } from '@codemirror/commands'
import { json } from '@codemirror/lang-json'
import { sql } from '@codemirror/lang-sql'
import { HighlightStyle, foldGutter, syntaxHighlighting } from '@codemirror/language'
import { EditorState, type Extension } from '@codemirror/state'
import { EditorView, keymap, lineNumbers } from '@codemirror/view'
import { tags } from '@lezer/highlight'
import { useEffect, useRef } from 'preact/hooks'

import './editor.css'

const theme = EditorView.theme(
  {
    '&': {
      color: 'var(--color-text-1)',
      backgroundColor: 'var(--color-surface-1)',
      fontSize: 'var(--text-sm)',
    },
    '.cm-scroller': {
      fontFamily: 'var(--font-mono)',
    },
    '.cm-gutters': {
      color: 'var(--color-text-3)',
      backgroundColor: 'var(--color-surface-1)',
      border: 'none',
    },
    '&.cm-focused': {
      outline: 'none',
    },
    '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
      backgroundColor: 'var(--color-surface-3)',
    },
  },
)

const highlight = HighlightStyle.define([
  { tag: tags.propertyName, color: 'var(--color-text-2)' },
  { tag: tags.string, color: 'var(--color-text-1)' },
  { tag: [tags.punctuation, tags.bracket], color: 'var(--color-text-3)' },
])

export type Language = 'json' | 'sql' | 'text'

const LANGUAGES: Record<Language, () => Extension[]> = {
  json: () => [json(), foldGutter(), syntaxHighlighting(highlight)],
  sql: () => [sql(), syntaxHighlighting(highlight)],
  text: () => [],
}

interface Handlers {
  onChange?: (text: string) => void
  onSubmit?: (text: string) => void
}

interface Props extends Handlers {
  text: string
  language: Language
}

export default function Editor({ text, language, onChange, onSubmit }: Props) {
  const parent = useRef<HTMLDivElement>(null)
  const view = useRef<EditorView | null>(null)
  const handlers = useRef<Handlers>({})
  handlers.current = { onChange, onSubmit }
  const editable = onChange !== undefined

  useEffect(() => {
    const created = new EditorView({
      parent: parent.current ?? undefined,
      state: EditorState.create({
        doc: text,
        extensions: [
          EditorState.readOnly.of(!editable),
          EditorView.lineWrapping,
          lineNumbers(),
          theme,
          ...LANGUAGES[language](),
          ...(editable ? edits(handlers) : []),
        ],
      }),
    })
    view.current = created

    return () => {
      created.destroy()
      view.current = null
    }
  }, [language, editable])

  useEffect(() => {
    const current = view.current
    if (current && !editable && current.state.doc.toString() !== text) {
      current.dispatch({ changes: { from: 0, to: current.state.doc.length, insert: text } })
    }
  }, [text, editable])

  return <div class="editor" ref={parent} />
}

function edits(handlers: { current: Handlers }): Extension[] {
  const submit = (view: EditorView) => {
    handlers.current.onSubmit?.(view.state.doc.toString())
    return true
  }

  return [
    history(),
    EditorView.theme({ '.cm-content': { minHeight: 'calc(var(--space-7) * 3)' } }),
    keymap.of([{ key: 'Mod-Enter', run: submit }, ...defaultKeymap, ...historyKeymap]),
    EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        handlers.current.onChange?.(update.state.doc.toString())
      }
    }),
  ]
}
