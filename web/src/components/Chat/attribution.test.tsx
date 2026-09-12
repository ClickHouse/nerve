import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { ChatMessage } from '../../types/chat';

/**
 * What attribution looks like on a transcript.
 *
 * The rule these specs pin down is that a label earns its place only when it
 * tells two things apart: the agent's own principal always does, and a person
 * does once a second person has spoken. Everything else renders exactly as it
 * did before attribution existed — which is not a nicety, it is most of the
 * screen. Every assistant row, every tool row and all history from before the
 * columns existed carry no actor, and turning those into "unknown user" chips
 * would be a worse UI than having no attribution at all.
 *
 * The other half is that no name is ever stored. A rename has to change every
 * label here while leaving the message objects byte for byte identical, which
 * is asserted directly rather than inferred.
 */

vi.mock('../../api/client', () => ({
  api: { listActors: vi.fn(), getActor: vi.fn() },
  getToken: vi.fn(() => 'tok'),
}));

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { MessageList } = await import('./MessageList');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, profile_version: 1, ...overrides };
}

const alice = (name: string | null = 'Alice') => actorRef(ALICE, { display_name: name });
const bob = (name: string | null = 'Bob') => actorRef(BOB, { display_name: name });
const system = (name: string | null = 'Nerve') =>
  actorRef(SYSTEM, { kind: 'system', display_name: name });

function said(text: string, actor_id: string | null, id = 1): ChatMessage {
  return { id, role: 'user', blocks: [{ type: 'text', content: text }], actor_id };
}

function replied(text: string, id = 2): ChatMessage {
  return { id, role: 'assistant', blocks: [{ type: 'text', content: text }] };
}

function renderTranscript(messages: ChatMessage[]) {
  return render(
    <MessageList messages={messages} streamingBlocks={[]} isStreaming={false} />,
  );
}

/** Every attribution label currently on screen, in DOM order. */
function labels(): string[] {
  return [...document.querySelectorAll('[data-attribution]')]
    .map((el) => el.textContent ?? '');
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
});

describe('a message with a sender', () => {
  it('is named once a second person has spoken', async () => {
    renderTranscript([said('first', ALICE, 1), said('second', BOB, 2)]);

    expect(await screen.findByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Bob')).toBeInTheDocument();
  });

  it('names the sender even on the messages that came before the second person', async () => {
    // The earlier messages were not ambiguous when they were sent. They are
    // now, so they get names too — the label describes the transcript's
    // current state, not the state at the time of writing.
    renderTranscript([
      said('one', ALICE, 1), said('two', ALICE, 2), said('three', BOB, 3),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Alice', 'Bob']));
  });

  it('is not named when one person is talking to themselves', async () => {
    renderTranscript([said('one', ALICE, 1), said('two', ALICE, 2)]);

    await waitFor(() => expect(listActors).toHaveBeenCalled());
    expect(labels()).toEqual([]);
    expect(screen.queryByText('Alice')).toBeNull();
  });

  it('shows a display name it does not have as the neutral fallback', async () => {
    listActors.mockResolvedValue({ actors: [alice(null), bob()] });
    renderTranscript([said('one', ALICE, 1), said('two', BOB, 2)]);

    expect(await screen.findByText('Unnamed account')).toBeInTheDocument();
    // Never the raw id in the label itself...
    expect(screen.queryByText(ALICE)).toBeNull();
    // ...but it is in the tooltip, which is what tells "this account has no
    // name" apart from "this id is from somewhere else".
    expect(screen.getByTitle(`Sent by actor ${ALICE}`)).toBeInTheDocument();
  });

  it('renders an id the server has never heard of rather than hiding the message', async () => {
    // Two namespaces in one list is a legitimate state after a later move to
    // external identity: the same person is two actors and one of them is not
    // in this instance's table at all.
    listActors.mockResolvedValue({ actors: [alice()] });
    renderTranscript([said('mine', ALICE, 1), said('theirs', 'actor-elsewhere', 2)]);

    expect(await screen.findByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Unnamed account')).toBeInTheDocument();
    // The message itself is still there — a name that cannot be resolved must
    // never cost the reader the content.
    expect(screen.getByText('theirs')).toBeInTheDocument();
  });

  it('asks for an unknown id once and does not spin on it', async () => {
    listActors.mockResolvedValue({ actors: [alice()] });
    renderTranscript([said('mine', ALICE, 1), said('theirs', 'actor-elsewhere', 2)]);

    await screen.findByText('Unnamed account');
    await new Promise((r) => setTimeout(r, 20));
    expect(listActors.mock.calls.length).toBeLessThanOrEqual(2);
  });
});

describe('the agent itself', () => {
  it('is named on a message it sent, even with no person to compare it to', async () => {
    renderTranscript([said('run the sweep', SYSTEM, 1)]);

    expect(await screen.findByText('Nerve')).toBeInTheDocument();
  });

  it('says what it is, not just who it is', async () => {
    renderTranscript([said('run the sweep', SYSTEM, 1)]);

    await screen.findByText('Nerve');
    expect(
      screen.getByTitle('Sent by Nerve itself — scheduled or autonomous work'),
    ).toBeInTheDocument();
  });

  it('carries a glyph as well as the wording, marked decorative', async () => {
    const { container } = renderTranscript([said('run the sweep', SYSTEM, 1)]);

    await screen.findByText('Nerve');
    const label = container.querySelector('[data-attribution="message"]')!;
    const glyph = label.querySelector('svg')!;
    // The name is right beside it, so the glyph adds nothing for a screen
    // reader and must not be announced.
    expect(glyph).toHaveAttribute('aria-hidden', 'true');
  });

  it('falls back to its own name when bootstrap gave it none', async () => {
    listActors.mockResolvedValue({ actors: [system(null)] });
    renderTranscript([said('run the sweep', SYSTEM, 1)]);

    expect(await screen.findByText('Nerve')).toBeInTheDocument();
  });
});

describe('a message with no sender', () => {
  it('renders exactly as it always has — no chip, no placeholder', async () => {
    renderTranscript([said('old message', null, 1), said('another', null, 2)]);

    await new Promise((r) => setTimeout(r, 20));
    expect(labels()).toEqual([]);
    expect(screen.queryByText('Unnamed account')).toBeNull();
    expect(screen.getByText('old message')).toBeInTheDocument();
  });

  it('asks the server for nothing at all', async () => {
    renderTranscript([said('old message', null, 1), replied('an answer', 2)]);

    await new Promise((r) => setTimeout(r, 20));
    expect(listActors).not.toHaveBeenCalled();
  });

  it('stays unnamed while the people around it are named', async () => {
    renderTranscript([
      said('history', null, 1), said('one', ALICE, 2), said('two', BOB, 3),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Bob']));
    expect(screen.getByText('history')).toBeInTheDocument();
  });
});

describe('assistant rows', () => {
  it('carry no attribution markup anywhere inside them', async () => {
    const { container } = renderTranscript([
      said('ask', ALICE, 1), replied('answer', 2), said('ask again', BOB, 3),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Bob']));
    // Both people are named, so this is the loudest attribution gets — and the
    // assistant turn between them still has nothing on it.
    const assistant = container.querySelector('[data-role="assistant"]')!;
    expect(assistant.querySelectorAll('[data-attribution]')).toHaveLength(0);
    expect(assistant.textContent).not.toContain('Alice');
    expect(assistant.textContent).not.toContain('Nerve');
  });
});

describe('renaming somebody', () => {
  it('changes every label without touching a single message object', async () => {
    const messages = [said('one', ALICE, 1), said('two', BOB, 2)];
    const before = structuredClone(messages);
    renderTranscript(messages);
    expect(await screen.findByText('Alice')).toBeInTheDocument();

    // What the accounts screen does after a rename: a fresh read, with the
    // new name and a bumped profile version.
    listActors.mockResolvedValue({
      actors: [alice('Alice Doe'), { ...bob(), profile_version: 3 }],
    });
    await act(() => useActorStore.getState().refresh());

    expect(await screen.findByText('Alice Doe')).toBeInTheDocument();
    expect(screen.queryByText('Alice')).toBeNull();
    // The rows the label came from are untouched: a name was never stored on
    // one, so there was nothing to rewrite.
    expect(messages).toEqual(before);
    expect(messages[0].actor_id).toBe(ALICE);
  });
});
