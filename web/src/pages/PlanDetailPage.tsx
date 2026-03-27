import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Check, X, MessageSquare, ExternalLink, Users, ChevronDown } from 'lucide-react';
import { usePlanStore } from '../stores/planStore';
import { MarkdownContent } from '../components/Chat/MarkdownContent';
import { api } from '../api/client';
import { PLAN_STATUS_STYLES as STATUS_STYLES, PLAN_TYPE_STYLES as TYPE_STYLES } from '../constants/statusStyles';

interface HoaStatus {
  enabled: boolean;
  available: boolean;
  version: string | null;
  default_mode: string;
  default_agents: string[];
}

export function PlanDetailPage() {
  const { planId } = useParams<{ planId: string }>();
  const navigate = useNavigate();
  const { selectedPlan: plan, detailLoading, actionLoading, loadPlan, updatePlan, approvePlan, revisePlan, clearSelectedPlan } = usePlanStore();
  const [feedback, setFeedback] = useState('');
  const [showFeedback, setShowFeedback] = useState(false);

  // houseofagents runtime selection state
  const [hoaStatus, setHoaStatus] = useState<HoaStatus | null>(null);
  const [useMultiAgent, setUseMultiAgent] = useState(false);
  const [hoaMode, setHoaMode] = useState('relay');
  const [hoaAgents, setHoaAgents] = useState('');

  useEffect(() => {
    if (planId) loadPlan(planId);
    // Check houseofagents availability
    api.getHoaStatus().then(setHoaStatus).catch(() => {});
    return () => clearSelectedPlan();
  }, [planId]);

  // Pre-fill defaults when status loads
  useEffect(() => {
    if (hoaStatus?.enabled) {
      setHoaMode(hoaStatus.default_mode);
      setHoaAgents(hoaStatus.default_agents.join(', '));
    }
  }, [hoaStatus]);

  if (detailLoading || !plan) {
    return (
      <div className="h-full flex items-center justify-center text-[#444]">
        {detailLoading ? 'Loading...' : 'Plan not found'}
      </div>
    );
  }

  const isPending = plan.status === 'pending';
  const isImplementing = plan.status === 'implementing';
  const hoaAvailable = hoaStatus?.enabled && hoaStatus?.available;

  const handleApprove = async () => {
    const options = useMultiAgent ? {
      runtime: 'houseofagents' as const,
      hoa_mode: hoaMode,
      hoa_agents: hoaAgents.split(',').map(a => a.trim()).filter(Boolean),
    } : undefined;

    const result = await approvePlan(plan.id, options);
    if (result?.impl_session_id) {
      navigate(`/chat/${result.impl_session_id}`);
    }
  };

  const handleDecline = () => {
    updatePlan(plan.id, 'declined');
  };

  const handleRevise = () => {
    if (feedback.trim()) {
      revisePlan(plan.id, feedback.trim());
      setFeedback('');
      setShowFeedback(false);
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-[#222] px-6 py-3 bg-[#0f0f0f] shrink-0">
        <div className="flex items-center gap-3 mb-2">
          <button
            onClick={() => navigate('/plans')}
            className="p-1 text-[#555] hover:text-[#aaa] hover:bg-[#1f1f1f] rounded cursor-pointer"
          >
            <ArrowLeft size={16} />
          </button>
          <h1 className="text-lg font-semibold text-[#e0e0e0]">
            {plan.task_title || plan.task_id}
          </h1>
          <span className={`px-2 py-0.5 text-[12px] rounded-full border ${STATUS_STYLES[plan.status] || STATUS_STYLES.superseded}`}>
            {plan.status}
          </span>
          {plan.plan_type && plan.plan_type !== 'generic' && TYPE_STYLES[plan.plan_type] && (
            <span className={`px-2 py-0.5 text-[11px] rounded-full border ${TYPE_STYLES[plan.plan_type].className}`}>
              {TYPE_STYLES[plan.plan_type].label}
            </span>
          )}
          <span className="text-[12px] text-[#555]">v{plan.version}</span>
        </div>
        <div className="flex items-center gap-4 text-[12px] text-[#555] ml-7">
          <span>{plan.created_at?.slice(0, 16).replace('T', ' ')}</span>
          {plan.model && <span>{plan.model}</span>}
          <button
            onClick={() => navigate(`/tasks/${plan.task_id}`)}
            className="flex items-center gap-1 text-[#6366f1] hover:text-[#818cf8] cursor-pointer"
          >
            <ExternalLink size={11} /> View task
          </button>
          {isImplementing && plan.impl_session_id && (
            <button
              onClick={() => navigate(`/chat/${plan.impl_session_id}`)}
              className="flex items-center gap-1 text-blue-400 hover:text-blue-300 cursor-pointer"
            >
              <MessageSquare size={11} /> Watch implementation
            </button>
          )}
        </div>
      </div>

      {/* Plan content */}
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-3xl mx-auto">
          <div className="bg-[#141414] border border-[#222] rounded-lg p-6">
            <MarkdownContent content={plan.content} />
          </div>

          {/* Feedback from previous revision — quote style */}
          {plan.feedback && (
            <div className="mt-4 flex gap-0">
              <div className="w-1 bg-[#6366f1]/40 rounded-full shrink-0" />
              <div className="pl-3 py-2">
                <div className="text-[11px] text-[#6366f1]/60 font-medium mb-1">Revision feedback</div>
                <div className="text-[13px] text-[#aaa] leading-relaxed whitespace-pre-wrap">{plan.feedback}</div>
              </div>
            </div>
          )}

          {/* Action bar for pending plans */}
          {isPending && (
            <div className="mt-6 space-y-3">
              {/* Multi-agent toggle (only when houseofagents is available) */}
              {hoaAvailable && (
                <div className="space-y-2">
                  <button
                    onClick={() => setUseMultiAgent(!useMultiAgent)}
                    className={`flex items-center gap-2 px-3 py-1.5 text-[12px] rounded-lg border cursor-pointer transition-colors ${
                      useMultiAgent
                        ? 'bg-amber-400/10 text-amber-400 border-amber-400/30'
                        : 'bg-[#1a1a1a] text-[#666] border-[#333] hover:border-[#444]'
                    }`}
                  >
                    <Users size={13} />
                    Multi-Agent
                    <ChevronDown size={12} className={`transition-transform ${useMultiAgent ? 'rotate-180' : ''}`} />
                  </button>

                  {useMultiAgent && (
                    <div className="flex gap-3 pl-1">
                      <div className="flex items-center gap-1.5">
                        <label className="text-[11px] text-[#555]">Mode</label>
                        <select
                          value={hoaMode}
                          onChange={e => setHoaMode(e.target.value)}
                          className="px-2 py-1 text-[12px] bg-[#1a1a1a] border border-[#333] rounded text-[#ccc] focus:outline-none focus:border-[#6366f1]/50"
                        >
                          <option value="relay">Relay</option>
                          <option value="swarm">Swarm</option>
                          <option value="pipeline">Pipeline</option>
                        </select>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <label className="text-[11px] text-[#555]">Agents</label>
                        <input
                          value={hoaAgents}
                          onChange={e => setHoaAgents(e.target.value)}
                          placeholder="Claude, OpenAI"
                          className="px-2 py-1 text-[12px] bg-[#1a1a1a] border border-[#333] rounded text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#6366f1]/50 w-40"
                        />
                      </div>
                    </div>
                  )}
                </div>
              )}

              <div className="flex items-center gap-3">
                <button
                  onClick={handleApprove}
                  disabled={actionLoading}
                  className={`flex items-center gap-1.5 px-4 py-2 text-[13px] disabled:opacity-50 text-white rounded-lg cursor-pointer ${
                    useMultiAgent
                      ? 'bg-amber-600 hover:bg-amber-500'
                      : 'bg-emerald-600 hover:bg-emerald-500'
                  }`}
                >
                  {useMultiAgent ? <Users size={14} /> : <Check size={14} />}
                  {useMultiAgent ? 'Approve (Multi-Agent)' : 'Approve & Implement'}
                </button>
                <button
                  onClick={handleDecline}
                  disabled={actionLoading}
                  className="flex items-center gap-1.5 px-4 py-2 text-[13px] bg-red-600/80 hover:bg-red-500/80 disabled:opacity-50 text-white rounded-lg cursor-pointer"
                >
                  <X size={14} /> Decline
                </button>
                <button
                  onClick={() => setShowFeedback(!showFeedback)}
                  className="flex items-center gap-1.5 px-4 py-2 text-[13px] bg-[#2a2a2a] hover:bg-[#333] text-[#ccc] rounded-lg cursor-pointer"
                >
                  <MessageSquare size={14} /> Request Revision
                </button>
              </div>

              {showFeedback && (
                <div className="space-y-2">
                  <div className="flex gap-0">
                    <div className="w-1 bg-[#6366f1]/30 rounded-full shrink-0" />
                    <div className="flex-1 pl-3">
                      <textarea
                        value={feedback}
                        onChange={e => setFeedback(e.target.value)}
                        placeholder="Describe what to change..."
                        className="w-full p-3 text-[13px] bg-[#1a1a1a] border border-[#333] rounded-lg text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#6366f1]/50 resize-none"
                        rows={3}
                        autoFocus
                      />
                    </div>
                  </div>
                  <div className="flex justify-end">
                    <button
                      onClick={handleRevise}
                      disabled={actionLoading || !feedback.trim()}
                      className="px-4 py-2 text-[13px] bg-[#6366f1] hover:bg-[#818cf8] disabled:opacity-50 text-white rounded-lg cursor-pointer"
                    >
                      Send Revision Request
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
