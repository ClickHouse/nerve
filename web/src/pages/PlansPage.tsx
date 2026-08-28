import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Lightbulb } from '../components/ui/icons';
import { usePlanStore, type Plan } from '../stores/planStore';
import { Badge, type BadgeTone, Button, PageHeader } from '../components/ui';

/** A plan's lifecycle state, as a `Badge` tone. `superseded` is the fallback. */
const STATUS_TONES: Record<string, BadgeTone> = {
  pending: 'warning',
  approved: 'success',
  implementing: 'info',
  declined: 'danger',
  superseded: 'neutral',
  failed: 'danger',
};

/** Plan type is identity, not status — hence `purple` rather than a feedback tone. */
const TYPE_LABELS: Record<string, string> = {
  'skill-create': 'Skill',
  'skill-update': 'Skill Update',
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
          <h3 className="font-medium text-base text-text mb-1">
            {plan.task_title || plan.task_id}
          </h3>
          <div className="flex items-center gap-3 text-xs">
            <Badge size="sm" pill outline tone={STATUS_TONES[plan.status] ?? 'neutral'}>
              {plan.status}
            </Badge>
            {plan.plan_type && plan.plan_type !== 'generic' && TYPE_LABELS[plan.plan_type] && (
              <Badge size="sm" pill outline tone="purple">
                {TYPE_LABELS[plan.plan_type]}
              </Badge>
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
      <PageHeader
        icon={<Lightbulb size={18} className="text-accent shrink-0" />}
        title="Plans"
        filters={FILTERS.map(f => (
          <Button
            key={f.value}
            variant="pill"
            active={filter === f.value}
            onClick={() => setFilter(f.value)}
          >
            {f.label}
          </Button>
        ))}
      />

      <div className="flex-1 overflow-y-auto p-4 md:p-6">
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
