import { useEffect, useState } from 'react';
import { RefreshCw, Loader2 } from '../components/ui/icons';
import { IconButton } from '../components/ui';
import { useCronStore } from '../stores/cronStore';
import { CronSidebar } from '../components/Cron/CronSidebar';
import { JobInfoCard } from '../components/Cron/JobInfoCard';
import { LogsTable } from '../components/Cron/LogsTable';
import { PageHeader } from '../components/ui/PageHeader';
import { PaneToggle } from '../components/ui/PaneToggle';
import { Drawer } from '../components/ui/Drawer';
import { useIsMobile } from '../hooks/useMediaQuery';

export function CronPage() {
  const { jobs, selectedJobId, loadJobs, loadLogs, refresh } = useCronStore();
  const [refreshing, setRefreshing] = useState(false);

  // The job list is "which item within this section", so on a phone it
  // becomes a left drawer — the same anchor and the same toggle as the chat
  // session list, rather than a 220px column squeezing the run table to
  // roughly two visible columns.
  const isMobile = useIsMobile();
  const [listOpen, setListOpen] = useState(false);

  // Picking a job shuts the drawer, but that is driven by `CronSidebar`'s
  // `onSelect` rather than by watching `selectedJobId`: tapping the job that
  // is already selected — "All Jobs", most often — leaves the id unchanged,
  // and the drawer would stay over the table it was asked to reveal.
  //
  // All that is left here is retiring the drawer when the layout leaves the
  // phone breakpoint, so a later resize back down doesn't arrive with an
  // overlay already open. Adjusted during render rather than in an effect:
  // an effect paints the stale state for a frame first.
  const [lastIsMobile, setLastIsMobile] = useState(isMobile);
  if (lastIsMobile !== isMobile) {
    setLastIsMobile(isMobile);
    setListOpen(false);
  }

  useEffect(() => {
    loadJobs();
    loadLogs();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleRefresh = async () => {
    if (refreshing) return;
    setRefreshing(true);
    try { await refresh(); } finally { setRefreshing(false); }
  };

  const selectedJob = selectedJobId ? jobs.find(j => j.id === selectedJobId) : null;

  return (
    <div className="h-full flex flex-col">
      <PageHeader
        leading={isMobile
          ? <PaneToggle open={listOpen} onToggle={() => setListOpen(o => !o)} label="job list" />
          : undefined}
        title="Cron Jobs"
        actions={
          <IconButton label="Refresh" onClick={handleRefresh} disabled={refreshing}>
            {refreshing ? <Loader2 size={16} className="animate-spin" /> : <RefreshCw size={16} />}
          </IconButton>
        }
      />

      {/* Body */}
      <div className="flex-1 flex min-h-0">
        {isMobile ? (
          <Drawer open={listOpen} onClose={() => setListOpen(false)} side="left" label="Cron jobs">
            <CronSidebar inDrawer onSelect={() => setListOpen(false)} />
          </Drawer>
        ) : (
          <CronSidebar />
        )}

        <div className="flex-1 flex flex-col min-w-0">
          {selectedJob && <JobInfoCard job={selectedJob} />}
          <div className={`flex-1 flex flex-col min-h-0 ${selectedJob ? 'mt-2' : ''}`}>
            <LogsTable showJobColumn={selectedJobId === null} />
          </div>
        </div>
      </div>
    </div>
  );
}
