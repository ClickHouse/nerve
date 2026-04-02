import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Lightbulb } from 'lucide-react';
import { usePlanStore, type Plan } from '../stores/planStore';

const STATUS_STYLES: Record<string, string> = {
  pending: 'bg-yellow-400/10 text-yellow-400 border-yellow-400/20',
  approved: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  implementing: 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  declined: 'bg-red-400/10 text-red-400 border-red-400/20',
  superseded: 'bg-border-subtle/50 text-text-muted border-border-subtle',
  failed: 'bg-red-400/10 text-red-400 border-red-400/20',
};

const TYPE_STYLES: Record<string, { label: string; className: string }> = {
  'skill-create': { label: 'Skill', className: 'bg-purple-400/10 text-purple-400 border-purple-400/20' },
  'skill-update': { label: 'Skill Update', className: 'bg-purple-400/10 text-purple-300 border-purple-400/20' },
};

const FILTERS = [
  { label: 'All', value: '' },
  { label: 'Pending', value: 'pending' },
  { label: 'Approved', value: 'approved' },
  { label: 'Implementing', value: 'implementing' },
  { label: 'Declined', value: 'declined' },
];

function PlanCard({ plan }: { plan: Plan }) {
  const navigate = useNavigate();

  return (
    <div
      onClick={() => navigate(`/plans/${plan.id}`)}
      className="p-4 bg-surface border border-border-subtle rounded-lg hover:border-border transition-colors cursor-pointer"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <h3 className="font-medium text-[15px] text-text mb-1">
            {plan.task_title || plan.task_id}
          </h3>
          <div className="flex items-center gap-3 text-[12px]">
            <span className={`px-2 py-0.5 rounded-full border ${STATUS_STYLES[plan.status] || STATUS_STYLES.superseded}`}>
              {plan.status}
            </span>
            {plan.plan_type && plan.plan_type !== 'generic' && TYPE_STYLES[plan.plan_type] && (
              <span className={`px-2 py-0.5 rounded-full border text-[11px] ${TYPE_STYLES[plan.plan_type].className}`}>
                {TYPE_STYLES[plan.plan_type].label}
              </span>
            )}
            <span className="text-text-faint">v{plan.version}</span>
            <span className="text-text-faint">{plan.created_at?.slice(0, 10)}</span>
            {plan.model && <span className="text-text-faint">{plan.model}</span>}
          </div>
        </div>
      </div>
    </div>
  );
}

export function PlansPage() {
  const { plans, filter, loading, loadPlans, setFilter } = usePlanStore();

  useEffect(() => { loadPlans(); }, []);

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-border-subtle px-6 py-3 flex items-center gap-4 bg-bg shrink-0">
        <Lightbulb size={18} className="text-[#6366f1]" />
        <h1 className="text-lg font-semibold">Plans</h1>
        <div className="flex items-center gap-1 ml-2">
          {FILTERS.map(f => (
            <button
              key={f.value}
              onClick={() => setFilter(f.value)}
              className={`px-3 py-1 text-[12px] rounded-full border cursor-pointer transition-colors
                ${filter === f.value
                  ? 'bg-[#6366f1]/15 text-[#6366f1] border-[#6366f1]/30'
                  : 'text-text-dim border-border hover:border-border hover:text-text-muted'
                }`}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="text-text-faint text-center py-10">Loading...</div>
        ) : plans.length === 0 ? (
          <div className="text-text-faint text-center py-10">
            {filter ? `No ${filter} plans` : 'No plans yet. The task planner cron will propose plans automatically.'}
          </div>
        ) : (
          <div className="max-w-3xl mx-auto space-y-2">
            {plans.map(plan => (
              <PlanCard key={plan.id} plan={plan} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
