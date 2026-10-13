import { useEffect, useState } from 'react';
import {
  api, type DetectedAgent, type SetupAgentsDetail, type SetupState, type SetupStep,
} from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { Badge, Checkbox } from '../../ui';
import { Terminal } from '../../ui/icons';
import { ChoiceCards } from '../ChoiceCards';
import { AGENT_CONFLICT_POLICIES, AGENT_WRITE_EXISTING, AGENTS_WRITE_NOTHING, STEP_HEADINGS } from '../copy';
import { StepFrame } from '../StepFrame';

type Policy = 'backup' | 'skip' | 'merge';

const LABELS: Record<string, string> = { codex: 'Codex', 'claude-code': 'Claude Code' };

/**
 * Codex / Claude Code on this server: detected with versions, toggles, and
 * a conflict policy when a selected agent already has a config file.
 * Always sends `{agents, conflict_policy}` — `[]` is a decision.
 *
 * Nothing is pre-checked. Connecting an agent rewrites a file *outside*
 * Nerve's own (`~/.codex/config.toml`, `~/.claude/settings.json`), and a
 * wizard must not do that on a reflexive Continue; only agents the server
 * already lists as connected open checked. The policy choice sits under
 * the row it applies to, and a line above the footer states exactly what
 * Continue will write.
 */
export function AgentsStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupAgentsDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const detected = useSetupStore((s) => s.agentsDetected);
  const loadAgents = useSetupStore((s) => s.loadAgents);
  const locked = step.read_only || step.status === 'blocked';

  useEffect(() => { void loadAgents(); }, [loadAgents]);

  const rows: DetectedAgent[] = detected?.agents ?? detail.available.map((id) => ({
    id, label: LABELS[id] ?? id, installed: false, version: null, config_paths: [], config_exists: false,
    enabled: detail.enabled.includes(id),
  }));

  const [chosen, setChosen] = useState<string[]>(detail.enabled);
  const [policy, setPolicy] = useState<Policy>(
    (['backup', 'skip', 'merge'] as const).includes(detail.conflict_policy as Policy)
      ? detail.conflict_policy as Policy : 'backup',
  );

  const toggle = (id: string) => setChosen(
    chosen.includes(id) ? chosen.filter((x) => x !== id) : [...chosen, id],
  );

  const selectedRows = rows.filter((a) => chosen.includes(a.id));
  const firstWithConfig = selectedRows.find((a) => a.config_exists)?.id ?? null;
  const submit = () => save('agents', () => api.setupStep('agents', { agents: chosen, conflict_policy: policy }));
  const dirty = [...chosen].sort().join(',') !== [...detail.enabled].sort().join(',')
    || policy !== detail.conflict_policy;

  const writes = selectedRows.map((a) => {
    const paths = a.config_paths.length > 0 ? a.config_paths.join(' and ') : `${a.label}'s config`;
    return a.config_exists
      ? `Will write ${paths}; ${AGENT_WRITE_EXISTING[policy]}.`
      : `Will write ${paths} (new file).`;
  });

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.agents.heading}
      intro={STEP_HEADINGS.agents.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={dirty}
      aside={(
        <p className="text-xs text-text-muted" aria-live="polite">
          {writes.length === 0 ? AGENTS_WRITE_NOTHING : writes.join(' ')}
        </p>
      )}
    >
      <ul className="flex flex-col gap-1" aria-label="Agents">
        {rows.map((agent) => (
          <li key={agent.id} className="flex flex-col gap-2 rounded-lg border border-border-subtle px-3 py-2">
            <div className="flex items-start justify-between gap-3">
              <Checkbox
                checked={chosen.includes(agent.id)}
                onChange={() => toggle(agent.id)}
                disabled={locked}
                label={(
                  <span className="flex flex-col">
                    <span className="text-text flex items-center gap-2"><Terminal size={14} />{agent.label}</span>
                    <span className="text-xs text-text-dim">
                      {agent.installed ? (agent.version ?? 'installed') : 'not on PATH'}
                      {agent.config_paths.length > 0 && ` · ${agent.config_paths.join(', ')}`}
                    </span>
                  </span>
                )}
                labelClassName="items-start"
              />
              <span className="flex items-center gap-1 shrink-0">
                {agent.config_exists && <Badge tone="neutral">has config</Badge>}
                {agent.enabled && <Badge tone="success">connected</Badge>}
              </span>
            </div>
            {agent.id === firstWithConfig && (
              <div className="flex flex-col gap-2 pl-6">
                <h2 className="text-sm font-medium text-text">
                  {agent.label} already has a config file. What should happen to it?
                </h2>
                <ChoiceCards
                  label="Conflict policy"
                  options={AGENT_CONFLICT_POLICIES.map((p) => ({ value: p.value, title: p.title, description: p.description }))}
                  value={policy}
                  onChange={setPolicy}
                  disabled={locked}
                />
                {selectedRows.filter((a) => a.config_exists).length > 1 && (
                  <p className="text-xs text-text-dim">Applies to every selected agent that already has a config.</p>
                )}
              </div>
            )}
          </li>
        ))}
        {rows.length === 0 && <li className="text-xs text-text-dim">No external agents are known to this build.</li>}
      </ul>
      {!detected && <p className="text-xs text-text-dim" role="status">Looking for agents on the server…</p>}

      {fieldError && (fieldError.field === 'agents' || fieldError.field === 'conflict_policy') && (
        <p role="alert" className="text-sm text-error">{fieldError.detail}</p>
      )}
      {state.mode === 'worker' && <p className="text-xs text-text-dim">External agents do not apply to a worker instance.</p>}
    </StepFrame>
  );
}
