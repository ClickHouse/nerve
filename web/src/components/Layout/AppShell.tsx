import { Outlet } from 'react-router-dom';
import { NavRail } from './NavRail';
import { BottomNav } from './BottomNav';
import { useIsMobile } from '../../hooks/useMediaQuery';

export function AppShell() {
  const isMobile = useIsMobile();

  // Phones stack instead of splitting: 56px of permanent chrome is 14% of a
  // 412px viewport, and it is the width the transcript and every table need.
  // Exactly one of NavRail/BottomNav is mounted, so neither the notification
  // poll nor the feature-flag fetch they share runs twice.
  //
  // One tree for both layouts, with `<Outlet>` in a fixed position among its
  // siblings. Returning two different trees would swap the wrapper that owns
  // the outlet, so React would unmount and remount the whole active route on
  // every crossing of `md` — a rotation would throw away page state such as an
  // open dialog, a set of filters or half-typed form input.
  //
  // The nav stays first in the DOM in both layouts, which is where desktop
  // already had it; on a phone `BottomNav` paints itself last with `order-last`
  // while keeping that reading order.
  return (
    // h-dvh on a phone, not h-screen: the dynamic unit tracks the on-screen
    // keyboard, so the bar stays put instead of being pushed off the bottom.
    <div
      className={`flex bg-bg ${isMobile ? 'h-dvh flex-col' : 'h-screen'}`}
      // `viewport-fit=cover` lets the layout reach under the notch and the
      // rounded corners, which is what makes the background continuous — but it
      // also puts content there unless something pays the inset back. Doing it
      // once here covers every page laid out in this box; the bottom is left to
      // BottomNav, the element actually sitting against that edge.
      //
      // It does not reach the drawers: a `position: fixed` element is laid out
      // against the viewport, not against this padding box, so each of those
      // pays its own insets.
      style={isMobile ? {
        paddingTop: 'env(safe-area-inset-top)',
        paddingLeft: 'env(safe-area-inset-left)',
        paddingRight: 'env(safe-area-inset-right)',
      } : undefined}
    >
      {isMobile ? <BottomNav /> : <NavRail />}
      <div className="min-h-0 min-w-0 flex-1">
        <Outlet />
      </div>
    </div>
  );
}
