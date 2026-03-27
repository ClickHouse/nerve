import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Lightbulb } from 'lucide-react';
import { usePlanStore, type Plan } from '../stores/planStore';
import { PLAN_STATUS_STYLES as STATUS_STYLES, PLAN_TYPE_STYLES as TYPE_STYLES } from '../constants/statusStyles';

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
      className="p-4 bg-[#141414] border border-[#222] rounded-lg hover:border-[#444] transition-colors cursor-pointer"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <h3 className="font-medium text-[15px] text-[#e0e0e0] mb-1">
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
            <span className="text-[#555]">v{plan.version}</span>
            <span className="text-[#555]">{plan.created_at?.slice(0, 10)}</span>
            {plan.model && <span className="text-[#444]">{plan.model}</span>}
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
      <div className="border-b border-[#222] px-6 py-3 flex items-center gap-4 bg-[#0f0f0f] shrink-0">
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
                  : 'text-[#666] border-[#2a2a2a] hover:border-[#444] hover:text-[#999]'
                }`}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="text-[#444] text-center py-10">Loading...</div>
        ) : plans.length === 0 ? (
          <div className="text-[#444] text-center py-10">
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
