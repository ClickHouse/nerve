import type { ComponentType } from 'react';
import { Link, Navigate, useParams } from 'react-router-dom';
import { useIsMobile } from '../hooks/useMediaQuery';
import { NotificationsSection } from '../components/Settings/NotificationsSection';
import { SETTINGS_SECTIONS, type SettingsSection } from '../components/Settings/sections';
import { PageHeader } from '../components/ui';
import { ArrowLeft, ChevronRight, RotateCcw, Settings } from '../components/ui/icons';

const SECTION_BODIES: Record<string, ComponentType> = {
  notifications: NotificationsSection,
};

const sectionPath = (section: SettingsSection) => `/settings/${section.id}`;

/**
 * `/settings/:section?`: the section list and one section.
 *
 * Desktop shows the list on the left and opens the first section at
 * `/settings`. A phone shows the list at `/settings` and one section at
 * `/settings/<id>`, with a link back to the list. Setup stays one link away:
 * "Run setup again" under the list.
 */
export function SettingsPage() {
  const { section: sectionId } = useParams();
  const isMobile = useIsMobile();
  const section = SETTINGS_SECTIONS.find((s) => s.id === sectionId);

  if (sectionId && !section) return <Navigate to="/settings" replace />;
  if (!section && !isMobile) return <Navigate to={sectionPath(SETTINGS_SECTIONS[0])} replace />;

  const Body = section ? SECTION_BODIES[section.id] : null;
  return (
    <div className="flex h-full flex-1 flex-col overflow-hidden">
      <PageHeader icon={<Settings size={18} className="shrink-0 text-accent" />} title="Settings" />
      <div className="flex min-h-0 flex-1">
        {!isMobile && (
          <aside className="flex w-[220px] shrink-0 flex-col overflow-y-auto border-r border-border-subtle p-3">
            <SectionList current={section ?? null} />
          </aside>
        )}
        <div className="min-w-0 flex-1 overflow-auto p-4 lg:p-6">
          <div className="mx-auto flex max-w-2xl flex-col gap-4">
            {Body ? (
              <>
                {isMobile && (
                  <Link to="/settings" className="flex min-h-[44px] items-center gap-1.5 self-start text-sm text-text-muted hover:text-text">
                    <ArrowLeft size={14} aria-hidden="true" />
                    All settings
                  </Link>
                )}
                <Body />
              </>
            ) : (
              <SectionList current={null} />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function SectionList({ current }: { current: SettingsSection | null }) {
  const rowClass = 'flex min-h-[44px] items-center gap-3 rounded-lg px-3 py-2 transition-colors';
  return (
    <nav aria-label="Settings sections" className="flex flex-col gap-3">
      <ul className="flex flex-col gap-0.5">
        {SETTINGS_SECTIONS.map((section) => {
          const active = current?.id === section.id;
          const Icon = section.icon;
          return (
            <li key={section.id}>
              <Link
                to={sectionPath(section)}
                aria-current={active ? 'page' : undefined}
                className={`${rowClass} ${active ? 'bg-accent/10 text-text' : 'text-text-muted hover:bg-surface-hover hover:text-text'}`}
              >
                <Icon size={16} className="shrink-0" aria-hidden="true" />
                <span className="flex min-w-0 flex-1 flex-col">
                  <span className="text-sm">{section.label}</span>
                  <span className="text-xs text-text-dim md:hidden">{section.summary}</span>
                </span>
                <ChevronRight size={16} className="shrink-0 text-text-dim md:hidden" aria-hidden="true" />
              </Link>
            </li>
          );
        })}
      </ul>
      <div className="border-t border-border-subtle pt-3">
        <Link to="/setup" className={`${rowClass} text-text-muted hover:bg-surface-hover hover:text-text`}>
          <RotateCcw size={16} className="shrink-0" aria-hidden="true" />
          <span className="text-sm">Run setup again</span>
        </Link>
      </div>
    </nav>
  );
}
