import { useEffect, useState } from 'react';
import { RefreshCw, Loader2 } from 'lucide-react';
import { useCronStore } from '../stores/cronStore';
import { CronSidebar } from '../components/Cron/CronSidebar';
import { JobsOverview } from '../components/Cron/JobsOverview';
import { JobInfoCard } from '../components/Cron/JobInfoCard';
import { LogsTable } from '../components/Cron/LogsTable';

export function CronPage() {
  const { jobs, selectedJobId, loadJobs, loadLogs, refresh } = useCronStore();
  const [refreshing, setRefreshing] = useState(false);

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
      {/* Header */}
      <div className="border-b border-border-subtle px-4 py-2.5 flex items-center justify-between bg-bg shrink-0">
        <h1 className="text-lg font-semibold">Cron Jobs</h1>
        <button onClick={handleRefresh} disabled={refreshing}
          className="text-text-dim hover:text-text-muted cursor-pointer p-1.5 hover:bg-surface-raised rounded"
          title="Refresh">
          {refreshing ? <Loader2 size={16} className="animate-spin" /> : <RefreshCw size={16} />}
        </button>
      </div>

      {/* Body */}
      <div className="flex-1 flex min-h-0">
        <CronSidebar />

        <div className="flex-1 flex flex-col min-w-0">
          {selectedJob ? <JobInfoCard job={selectedJob} /> : <JobsOverview />}
          <div className="mt-2 px-4 pt-1 text-[11px] text-text-dim uppercase tracking-wide shrink-0">
            {selectedJob ? 'Run History' : 'Recent Runs'}
          </div>
          <LogsTable showJobColumn={selectedJobId === null} />
        </div>
      </div>
    </div>
  );
}
