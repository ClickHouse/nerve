import { create } from 'zustand';
import { api } from '../api/client';
import { isHosted } from '../api/hosted';
import type { McpAppsConfig } from '../types/mcpApps';

/**
 * Whether this Nerve hosts MCP Apps: the interactive views that MCP servers
 * give for their tools, and the widgets of an answer. `GET
 * /api/mcp-apps/config` says so. The feature is off by default, and off
 * until the answer is here, so a Nerve without it shows every tool call as
 * an ordinary card and asks nothing more than this one read.
 */
interface McpAppsState {
  /** The feature is on: the gateway serves the sandbox page at this address. */
  enabled: boolean;
  /** Read the config, once per page. A failed read can be tried again. */
  load: () => Promise<void>;
}

/**
 * True for a config that turns the feature on in this page.
 *
 * Behind the sign-in gateway (`hosted`) the feature stays off, whatever the
 * config says: there the session is a cookie that the browser attaches by
 * address, and the feature is not turned on for that deployment. This is
 * the web UI's switch; the gateway has its own
 * (`nerve/gateway/mcp_apps_sandbox.py`).
 */
export function featureEnabled(
  config: McpAppsConfig | null | undefined,
  hosted: boolean = isHosted(),
): boolean {
  return !hosted && !!config && config.enabled === true;
}

/** How long the one read may take; a gateway that says nothing by then has the feature off. */
const CONFIG_DEADLINE_MS = 10_000;

let reading: Promise<void> | null = null;

export const useMcpAppsStore = create<McpAppsState>((set) => ({
  enabled: false,

  load: () => {
    reading ??= (async () => {
      try {
        // Behind the gateway the answer could not turn it on: nothing to ask.
        const enabled = !isHosted()
          && featureEnabled(await api.getMcpAppsConfig(AbortSignal.timeout?.(CONFIG_DEADLINE_MS)));
        set({ enabled });
      } catch {
        // A gateway without this route, and a failed read, are the feature
        // being off. The next call reads again.
        reading = null;
        set({ enabled: false });
      }
    })();
    return reading;
  },
}));

/** Forget the read, for tests. */
export function resetMcpAppsConfig(): void {
  reading = null;
  useMcpAppsStore.setState({ enabled: false });
}
