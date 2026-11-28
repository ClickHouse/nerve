/** True when the text of a link says its address (an autolink, `www.` or a mail address). */
export function textIsAddress(text: string, href: string): boolean {
  const t = text.trim();
  return t === href || `http://${t}` === href || `https://${t}` === href || `mailto:${t}` === href;
}
