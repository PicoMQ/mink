import './logo.css'

export function Logo() {
  return (
    <svg class="logo" viewBox="0 0 11 11" role="img" aria-label="Mink">
      <rect class="logo-mark" width="11" height="11" />
      <path
        class="logo-glyph"
        stroke-width="1"
        stroke-linecap="butt"
        stroke-linejoin="bevel"
        d="M3.84 1.5 L1.5 3.84 L1.5 6.67 L6.67 1.5 L9.5 1.5 L1.5 9.5 L4.33 9.5 L9.5 4.33 L9.5 7.16 L7.16 9.5"
      />
    </svg>
  )
}
