import { getToken } from './client';

export type WSMessage =
  | { type: 'token'; session_id: string; content: string; parent_tool_use_id?: string }
  | { type: 'thinking'; session_id: string; content: string; parent_tool_use_id?: string }
  | { type: 'tool_use'; session_id: string; tool: string; input: Record<string, unknown>; tool_use_id?: string; parent_tool_use_id?: string }
  | { type: 'tool_result'; session_id: string; tool_use_id?: string; result: string; is_error?: boolean; parent_tool_use_id?: string }
  | { type: 'done'; session_id: string; usage?: { input_tokens?: number; output_tokens?: number; cache_creation_input_tokens?: number; cache_read_input_tokens?: number }; max_context_tokens?: number }
  | { type: 'stopped'; session_id: string }
  | { type: 'error'; session_id: string; error: string }
  | { type: 'session_switched'; session_id: string }
  | { type: 'session_updated'; session_id: string; title: string }
  | { type: 'session_status'; session_id: string; is_running: boolean; status?: string; buffered_events?: WSMessage[] }
  | { type: 'session_forked'; source_id: string; fork_id: string; title: string }
  | { type: 'session_resumed'; session_id: string }
  | { type: 'session_archived'; session_id: string }
  | { type: 'plan_update'; session_id: string; content: string }
  | { type: 'interaction'; session_id: string; interaction_id: string; interaction_type: 'question' | 'plan_exit' | 'plan_enter'; tool_name: string; tool_input: Record<string, unknown> }
  | { type: 'subagent_start'; session_id: string; tool_use_id: string; subagent_type: string; description: string; model?: string }
  | { type: 'subagent_complete'; session_id: string; tool_use_id: string; duration_ms: number; is_error?: boolean }
  | { type: 'file_changed'; session_id: string; path: string; operation: string; tool_use_id: string }
  | { type: 'notification'; notification_id: string; notification_type: 'notify' | 'question'; session_id: string; title: string; body: string; priority: string; options: string[] | null }
  | { type: 'notification_answered'; notification_id: string; session_id: string; answer: string; answered_by: string }
  | { type: 'answer_injected'; session_id: string; notification_id: string; title: string; answer: string; answered_by: string; content: string }
  | { type: 'session_running'; session_id: string; is_running: boolean }
  | { type: 'auto_resume_start'; session_id: string }
  | { type: 'background_tasks_update'; session_id: string; tasks: { task_id: string; label: string; tool: string; status: 'running' | 'done' | 'timeout' }[] }
  | { type: 'hoa_progress'; session_id: string; event: Record<string, unknown> }
  | { type: 'pong' };

type MessageHandler = (msg: WSMessage) => void;

export class NerveWebSocket {
  private ws: WebSocket | null = null;
  private handlers: Set<MessageHandler> = new Set();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingInterval: ReturnType<typeof setInterval> | null = null;
  private _connected = false;

  get connected() {
    return this._connected;
  }

  connect() {
    if (this.ws?.readyState === WebSocket.OPEN) return;

    const token = getToken();
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const url = `${protocol}//${host}/ws${token ? `?token=${token}` : ''}`;

    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      this._connected = true;
      this.startPing();
    };

    this.ws.onmessage = (event) => {
      try {
        const msg: WSMessage = JSON.parse(event.data);
        this.handlers.forEach((h) => h(msg));
      } catch {
        console.error('Failed to parse WS message:', event.data);
      }
    };

    this.ws.onclose = () => {
      this._connected = false;
      this.stopPing();
      this.scheduleReconnect();
    };

    this.ws.onerror = () => {
      this._connected = false;
    };
  }

  disconnect() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.stopPing();
    this.ws?.close();
    this.ws = null;
    this._connected = false;
  }

  send(data: Record<string, unknown>) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }

  sendMessage(content: string, sessionId: string) {
    this.send({ type: 'message', content, session_id: sessionId });
  }

  switchSession(sessionId: string) {
    this.send({ type: 'switch_session', session_id: sessionId });
  }

  stopSession(sessionId: string) {
    this.send({ type: 'stop', session_id: sessionId });
  }

  forkSession(sessionId: string, atMessageId?: string, title?: string) {
    this.send({ type: 'fork', session_id: sessionId, at_message_id: atMessageId, title });
  }

  resumeSession(sessionId: string) {
    this.send({ type: 'resume', session_id: sessionId });
  }

  answerInteraction(sessionId: string, interactionId: string, result: Record<string, string> | null, denied = false, message = '') {
    this.send({ type: 'answer_interaction', session_id: sessionId, interaction_id: interactionId, result, denied, message });
  }

  onMessage(handler: MessageHandler) {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  private startPing() {
    this.pingInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.send({ type: 'ping' });
      }
    }, 30000);
  }

  private stopPing() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, 3000);
  }
}

export const ws = new NerveWebSocket();
