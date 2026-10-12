/**
 * The box every wizard text control draws, whatever it sits on.
 *
 * The app's `FIELD_BASE` paints an input `bg-surface-raised` with a
 * `border-border-subtle` edge. On a raised card in the dark theme that box
 * matches both its parent and its own border, so the field is invisible
 * until it is focused — on the very first screen, Username and Setup token
 * were bare labels with nothing visible to type into.
 *
 * The wizard puts fields on the page, on raised cards and on the claim card,
 * so its controls take the recessed editing surface (darker than the page in
 * both themes: `#1d1d1d` under `#1f1f1c` dark, `#edeef1` under `#f6f7fa`
 * light) and the full-strength border. That is distinct from every surface
 * they can sit on, and the border is distinct from the box, in both themes.
 */
export const CONTROL_CLASS = 'bg-bg-sunken border-border';
